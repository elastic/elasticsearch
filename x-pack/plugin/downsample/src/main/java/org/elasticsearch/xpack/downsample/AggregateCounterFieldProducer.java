/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Base class that reads fields from the source index and produces their downsampled values
 */
class AggregateCounterFieldProducer extends AbstractDownsampleFieldProducer<SortedNumericDoubleValues> {

    private static final Logger logger = LogManager.getLogger(AggregateCounterFieldProducer.class);
    private double firstValue = Double.NaN;
    private double lastValue = Double.NaN;
    private double resetOffset = 0;

    AggregateCounterFieldProducer(String name) {
        super(name);
    }

    /**
     * Resets the producer to an empty value.
     */
    public void reset() {
        isEmpty = true;
        firstValue = Double.NaN;
        lastValue = Double.NaN;
        resetOffset = 0;
    }

    @Override
    public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
        throw new UnsupportedOperationException("collect(IntArrayList) is not supported");
    }

    public void collect(SortedNumericDoubleValues docValues, int docId) throws IOException {
        if (docValues.advanceExact(docId) == false) {
            return;
        }
        int docValuesCount = docValues.docValueCount();
        assert docValuesCount > 0;
        isEmpty = false;
        var currentValue = docValues.nextValue();
        // If this the first time we encounter a value
        if (Double.isNaN(lastValue)) {
            firstValue = currentValue;
            lastValue = currentValue;
            return;
        }

        // check for reset
        if (currentValue > firstValue) {
            resetOffset += firstValue;
        }
        firstValue = currentValue;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            logger.info(
                "Aggregated counter field '{}': {first_value: {}, last_value: {}, resetOffset: {}}",
                name(),
                firstValue,
                lastValue,
                resetOffset
            );
            builder.field(name(), firstValue + resetOffset);
        }
    }
}
