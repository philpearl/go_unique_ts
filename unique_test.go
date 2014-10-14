package go_unique_ts

import (
	"testing"
	"time"
)

func TestOrder(t *testing.T) {
	ts := time.Now().Unix()

	t1 := NewUniqueTimestamp(ts)
	t2 := NewUniqueTimestamp(ts + 1)

	if t1.String() >= t2.String() {
		t.Fatalf("Timestamps are not ordered correctly.  t1=%s, t2=%s", t1, t2)
	}

	if t1.String() >= MaxUniqueTimestamp(ts).String() {
		t.Fatalf("TS is greater than max %s > %s", t1, MaxUniqueTimestamp(ts))
	}

	if t1.String() <= MinUniqueTimestamp(ts).String() {
		t.Fatalf("TS is greater than max %s > %s", t1, MinUniqueTimestamp(ts))
	}

	if t2.String() <= MaxUniqueTimestamp(ts).String() {
		t.Fatalf("Max is greater than next ts")
	}

	if t1.String() >= MinUniqueTimestamp(ts+1).String() {
		t.Fatalf("Min is less than prev ts")
	}

}

func TestParse(t *testing.T) {
	ts := time.Now().Unix()

	t1 := NewUniqueTimestamp(ts)
	t2 := UniqueTimestamp{}

	t.Log(t1.String())

	err := t2.FromString(t1.String())
	if err != nil {
		t.Fatalf("Failed to parse: %b", err)
	}

	if t2.Timestamp != t1.Timestamp {
		t.Fatalf("timestamp not preserved by parsing")
	}
}
