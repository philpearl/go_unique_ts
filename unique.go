/*
Uniquified timestamps that are naturally ordered.

This is inspired by the use of type 1 UUIDs as unique timestamps in Cassandra as a way to make unique
primary keys from non-unique timestamps, but still be able to order by time.

UUID type 1 includes the timestamp, but in a reordered form that means the UUID natural order is not
time-order.  Cassandra gets around this by having a special order for TimestampUUID fields.  The idea
behind this package is to produce a UUID-type object with a string representation that is naturally
a time order

The format of the timestamps is as follows.

0000543cef9f-0000b9d-c42c0319bdbe

The first part is the timestamp in hex.  The high-order two bytes of the 64-but timestamp are not included.
The second part is a randomly initialised monotonically increasing sequence number.  The final part is the
machine mac address (or a random value if this is not available.)

Note if you use the package in multiple processes in the same machine it is not guaranteed unique.  Perhaps
I should use this as a hint to use a random value for the 3rd part instead of the MAC addresss.  This may
change soon...
*/
package go_unique_ts

import (
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	hwAddr [6]byte
	seqNo  uint32
)

func init() {
	hwAddrSet := false
	if interfaces, err := net.Interfaces(); err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagLoopback == 0 && len(i.HardwareAddr) > 0 {
				copy(hwAddr[:], i.HardwareAddr)
				hwAddrSet = true
				break
			}
		}
	}
	if !hwAddrSet {
		// If we failed to obtain the MAC address of the current computer,
		// we will use a randomly generated 6 byte sequence instead and set
		// the multicast bit as recommended in RFC 4122.
		_, err := io.ReadFull(rand.Reader, hwAddr[:])
		if err != nil {
			panic(err)
		}
		hwAddr[0] = hwAddr[0] | 0x01
	}

	// initialize the clock sequence with a random number
	var clockSeqRand [2]byte
	io.ReadFull(rand.Reader, clockSeqRand[:])
	seqNo = uint32(clockSeqRand[1])<<8 | uint32(clockSeqRand[0])
}

type UniqueTimestamp struct {
	// Timestamp - expected to be in seconds since 1 Jan 1970.  high-order 2 bytes are ignored.
	Timestamp int64
	seqNo     uint32
	hwAddr    [6]byte
}

/*
Create a new UniqueTimestamp for a given timestamp
*/
func NewUniqueTimestamp(timestamp int64) UniqueTimestamp {
	return UniqueTimestamp{
		Timestamp: timestamp,
		seqNo:     atomic.AddUint32(&seqNo, 1),
		hwAddr:    hwAddr,
	}
}

/*
Create a UniqueTimestamp whose string representation is guaranteed to be less than any other
UniqueTimestamp for the same timestamp, but still greater than any
UniqueTimestamp for a lower value of timestamp.
*/
func MinUniqueTimestamp(timestamp int64) UniqueTimestamp {
	return UniqueTimestamp{
		Timestamp: timestamp,
		seqNo:     0,
		hwAddr:    [6]byte{0, 0, 0, 0, 0, 0},
	}
}

/*
Create a UniqueTimestamp whose string representation is guaranteed to be greater than any other
UniqueTimestamp for the same timestamp, but still less than any
UniqueTimestamp for a greater value of timestamp.
*/
func MaxUniqueTimestamp(timestamp int64) UniqueTimestamp {
	return UniqueTimestamp{
		Timestamp: timestamp,
		seqNo:     0xFFFFFFFF,
		hwAddr:    [6]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	}
}

/*
Parse a UniqueTimestamp
*/
func (u *UniqueTimestamp) FromString(val string) error {
	parts := strings.Split(val, "-")
	if len(parts) != 3 {
		return fmt.Errorf("timestamp should contain 2 -")
	}

	timestamp, err := strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return fmt.Errorf("could not parse timestamp part: %v", err)
	}
	u.Timestamp = timestamp

	seqNo, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return fmt.Errorf("could not parse seqno part: %v", err)
	}
	u.seqNo = uint32(seqNo)

	for i := range u.hwAddr {
		b, err := strconv.ParseUint(parts[2][2*i:2*i+2], 16, 8)
		if err != nil {
			return fmt.Errorf("could not parse hw addr part at index %d: %v", 2*i, err)
		}
		u.hwAddr[i] = byte(b)
	}

	return nil
}

const hexString = "0123456789abcdef"

/*
Get the string representation of a UniqueTimestamp.
*/
func (u UniqueTimestamp) String() string {
	// Format is timestamp bytes - seq bytes - hwaddr bytes
	// Can ignore top 2 bytes of TS for a few hundred years
	// 12 - 8 - 12
	r := make([]byte, 34)
	for i := 0; i < 6; i++ {
		b := (u.Timestamp >> uint(40-(i*8))) & 0xFF
		r[2*i] = hexString[b>>4]
		r[2*i+1] = hexString[b&0xF]
	}
	r[12] = '-'
	for i := 0; i < 4; i++ {
		b := (u.seqNo >> uint(24-(i*8))) & 0xFF
		r[13+2*i] = hexString[b>>4]
		r[14+2*i] = hexString[b&0xF]
	}
	r[20] = '-'
	for i, b := range u.hwAddr {
		r[21+2*i] = hexString[b>>4]
		r[22+2*i] = hexString[b&0xF]
	}

	return string(r)
}
