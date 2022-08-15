package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tkrajina/gpxgo/gpx"
	"peakbagger-tools/pbtools/peakbagger"
	"peakbagger-tools/pbtools/track"
)

var (
	usernamePB = flag.String("username", "", "Peakbagger username")
	passwordPB = flag.String("password", "", "Peakbagger password")

	inputFile      = flag.String("filename", "", "Input GPS track file")
	inputDirectory = flag.String("directory", "", "Input directory")

	dryRun = flag.Bool("dry_run", false, "Dry run, don't upload ascents")
	retry = flag.Bool("retry", false, "Retry historic failures")

	// Maps file extension to gpsbabel input format string
	extToGPSBabelFormat = map[string]string{
		".gdb": "gdb",
		".gpx": "gpx",
		".kml": "kml",
		".kmz": "kmz",
	}
)

// Converts a provided file (of any supported GPS format) into a temporary GPX file
// The caller is responsible for deleting the temporary file
func ToGPX(inputFile string) (string, error) {
	ext := strings.ToLower(filepath.Ext(inputFile))

	format, ok := extToGPSBabelFormat[ext]
	if !ok {
		return "", fmt.Errorf("file extension %q is not not a known GPS format", ext)
	}

	of, err := ioutil.TempFile("", "peakbagger-bulk-uploader.*.gpx")
	outputFile := of.Name()
	of.Close()
	if err != nil {
		return "", fmt.Errorf("failed to create temp gpx output file: %v", err)
	}

	log.Infof("Converting %q to %q", inputFile, outputFile)
	cmd := exec.Command("gpsbabel", "-t", "-i", format, "-f", inputFile, "-x", "simplify,count=2900", "-o", "gpx,garminextensions", "-F", outputFile)

	if err := cmd.Run(); err != nil {
		out, _ := cmd.CombinedOutput()
		return outputFile, fmt.Errorf("gpsbabel conversion failed %v: %s", err, string(out))
	}

	return outputFile, nil
}

type TrackBounds struct {
	Start   *gpx.GPXPoint
	Highest *gpx.GPXPoint
	End     *gpx.GPXPoint
}

// Calculates the highest point from the provided GPX file
func ToTrackBounds(t gpx.GPXTrack) (*TrackBounds, error) {
	tb := &TrackBounds{}

	for _, segment := range t.Segments {
		for _, p := range segment.Points {
			point := &gpx.GPXPoint{}
			*point = p

			if tb.Start == nil {
				tb.Start = point
			}
			tb.End = point

			if tb.Highest == nil {
				tb.Highest = point
			}

			if point.Elevation.NotNull() && point.Elevation.Value() > tb.Highest.Elevation.Value() {
				tb.Highest = point
			}
		}
	}

	if tb.Highest == nil {
		return nil, fmt.Errorf("missing points")
	}

	if !tb.Highest.Elevation.NotNull() {
		return nil, fmt.Errorf("missing elevation")
	}

	if tb.Highest.Timestamp.IsZero() {
		return nil, fmt.Errorf("missing timestamp")
	}

	return tb, nil
}

type History struct {
	Error string
	Added time.Time
}

type Uploader struct {
	client *peakbagger.PeakBagger

	FilenameHistory map[string]*History
}

func NewUploader() (*Uploader, error) {
	pb := peakbagger.NewClient(*usernamePB, *passwordPB)
	climberID, err := pb.Login()
	if err != nil {
		return nil, fmt.Errorf("peakbagger login %w", err)
	}

	log.Infof("Logged in as %v", climberID)

	return &Uploader{
		client:          pb,
		FilenameHistory: make(map[string]*History),
	}, nil
}

func (u *Uploader) UploadTrack(t gpx.GPXTrack) error {
	tb, err := ToTrackBounds(t)
	if err != nil {
		return fmt.Errorf("highest point %w", err)
	}

	log.Infof("Highest point is %v", tb.Highest)

	bounds := track.Bounds{
		MinLat: tb.Highest.Latitude,
		MaxLat: tb.Highest.Latitude,
		MinLng: tb.Highest.Longitude,
		MaxLng: tb.Highest.Longitude,
	}

	// Allow 1000 of search area for peaks
	bounds = bounds.Extend(float64(1000) / float64(69*5280))

	peaks, err := u.client.FindPeaks(&bounds)
	if err != nil {
		return fmt.Errorf("find peaks %w", err)
	}

	// Sort by closest to our highest point
	sort.Slice(peaks, func(i, j int) bool {
		return gpx.Distance2D(peaks[i].Latitude, peaks[i].Longitude, tb.Highest.Latitude, tb.Highest.Longitude, true) <
			gpx.Distance2D(peaks[j].Latitude, peaks[j].Longitude, tb.Highest.Latitude, tb.Highest.Longitude, true)
	})

	log.Infof("Found %d matching peaks", len(peaks))
	if len(peaks) == 0 {
		return fmt.Errorf("no peaks found")
	}
	if len(peaks) > 1 {
		log.Warnf("expected 1 matching peak, found %d: %v. Using first.", len(peaks), peaks)
	}
	peak := peaks[0]
	log.Infof("Highest point corresponds to %q", peak.Name)

	ascents, err := u.client.ListAscents()
	if err != nil {
		return fmt.Errorf("list ascents %w", err)
	}

	log.Infof("Loaded %d ascents", len(ascents))

	if ascents.Has(peak.PeakID, &tb.Highest.Timestamp) {
		return fmt.Errorf("Already have ascent logged for %q on %v", peak.Name, tb.Highest.Timestamp)
	}

	times := t.TimeBounds()

	// TODO: split the track on uphill vs downhill, then trim tracks to remove stopped time at summit

	ascent := peakbagger.Ascent{
		PeakID:     peak.PeakID,
		Date:       &tb.Highest.Timestamp,
		Gpx:        &gpx.GPX{Tracks: []gpx.GPXTrack{t}},
		TripReport: fmt.Sprintf("[i]Uploaded by [a href=\"https://github.com/jheidel/peakbagger-bulk-uploader\"]peakbagger-bulk-uploader[/a] on %s[/i]", time.Now().Format(time.RFC3339Nano)),

		// TODO polish up some of the stats

		TimeUp:   tb.Highest.Timestamp.Sub(times.StartTime),
		TimeDown: times.EndTime.Sub(tb.Highest.Timestamp),

		StartElevation: tb.Start.Elevation.Value(),
		EndElevation:   tb.End.Elevation.Value(),
	}

	log.Infof("Adding ascent %v", ascent)

	if *dryRun {
		log.Infof("DRY RUN, skipping ascent add")
		return nil
	}

	if _, err := u.client.AddAscent(ascent); err != nil {
		return fmt.Errorf("failed to add ascent %w", err)
	}

	log.Infof("Uploaded new ascent for %q", peak.Name)

	return nil

}

func (u *Uploader) UploadFile(filename string) error {
	gf, err := ToGPX(filename)
	if err != nil {
		return fmt.Errorf("ToGPX failed %w", err)
	}
	defer func() {
		os.Remove(gf)
	}()

	b, err := ioutil.ReadFile(gf)
	if err != nil {
		return fmt.Errorf("read gpx file %w", err)
	}

	g, err := gpx.ParseBytes(b)
	if err != nil {
		return fmt.Errorf("parse gpx bytes %w", err)
	}

	var errAcc error
	for _, t := range g.Tracks {
		if err := u.UploadTrack(t); err != nil {
			err = fmt.Errorf("%v processing track %q", err, t.Name)
			if errAcc == nil {
				errAcc = err
			} else {
				errAcc = fmt.Errorf("%v, %v", errAcc, err)
			}
		}
	}
	return errAcc
}

const HistoryFilename = "history.json"

func (u *Uploader) LoadHistory() error {
	b, err := ioutil.ReadFile(path.Join(*inputDirectory, HistoryFilename))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, u)
}

func (u *Uploader) SaveHistory() error {
	b, err := json.MarshalIndent(u, "", " ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path.Join(*inputDirectory, HistoryFilename), b, 0644)
}

func (u *Uploader) Run() error {
	if *inputFile != "" {
		return u.UploadFile(*inputFile)
	}

	files, err := ioutil.ReadDir(*inputDirectory)
	if err != nil {
		return err
	}

	if err := u.LoadHistory(); err != nil {
		return err
	}

	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		if _, ok := extToGPSBabelFormat[path.Ext(fi.Name())]; !ok {
			// Skip unsupported formats
			continue
		}
		hist, ok := u.FilenameHistory[fi.Name()]
		if ok && (hist.Error == "" || !*retry) {
			log.Infof("Skipping already processed file %q", fi.Name())
			continue
		}
		err := u.UploadFile(path.Join(*inputDirectory, fi.Name()))
		v := ""
		if err != nil {
			v = err.Error()
		}
		u.FilenameHistory[fi.Name()] = &History{
			Error: v,
			Added: time.Now(),
		}

		if err := u.SaveHistory(); err != nil {
			return err
		}
	}
	return nil
}

// TODO:
// - identify multiple high points per track, try all
// - handle multiple tracks per gpx file
// - improve calculation of elevation gain, extra gain, time spent, etc
// - support selection if there are multiple peaks in the zone
// - identify duplicates in our own dataset (avoid repeated FindAscents calls)
// - compile all tracks into a mega dataset?

func main() {
	flag.Parse()

	// Configure logging.
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	log.Infof("Started!")

	u, err := NewUploader()
	if err != nil {
		log.Fatalf("%v", err)
	}
	if err := u.Run(); err != nil {
		log.Fatalf("%v", err)
	}
}
