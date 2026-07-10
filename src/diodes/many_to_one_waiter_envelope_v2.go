package diodes

import (
	gendiodes "code.cloudfoundry.org/go-diodes"
	"code.cloudfoundry.org/go-loggregator/v10/rpc/loggregator_v2"
)

// ManyToOneWaiterEnvelopeV2 diode is optimized for many writers and a single reader
type ManyToOneWaiterEnvelopeV2 struct {
	d *gendiodes.Waiter
}

// NewManyToOneWaiterEnvelopeV2 initializes a new many-to-one diode for V2
// envelopes of a given size and alerter. The alerter is called whenever data
// is dropped with an integer representing the number of V2 envelopes that
// were dropped.
func NewManyToOneWaiterEnvelopeV2(size int, alerter gendiodes.Alerter, opts ...gendiodes.WaiterConfigOption) *ManyToOneWaiterEnvelopeV2 {
	return &ManyToOneWaiterEnvelopeV2{
		d: gendiodes.NewWaiter(gendiodes.NewManyToOne(size, alerter), opts...),
	}
}

// Set inserts the given V2 envelope into the diode.
func (d *ManyToOneWaiterEnvelopeV2) Set(data *loggregator_v2.Envelope) {
	d.d.Set(gendiodes.GenericDataType(data))
}

// TryNext returns the next V2 envelope to be read from the diode. If the
// diode is empty it will return a nil envelope and false for the bool.
func (d *ManyToOneWaiterEnvelopeV2) TryNext() (*loggregator_v2.Envelope, bool) {
	data, ok := d.d.TryNext()
	if !ok {
		return nil, ok
	}

	return (*loggregator_v2.Envelope)(data), true
}

// Next will return the next V2 envelope to be read from the diode. If the
// diode is empty this method will block until anenvelope is available to be
// read.
func (d *ManyToOneWaiterEnvelopeV2) Next() *loggregator_v2.Envelope {
	data := d.d.Next()
	return (*loggregator_v2.Envelope)(data)
}
