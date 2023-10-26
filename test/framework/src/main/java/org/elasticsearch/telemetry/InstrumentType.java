/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.telemetry.metric.DoubleCounter;
import org.elasticsearch.telemetry.metric.DoubleGauge;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleUpDownCounter;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;

import java.util.Objects;

/**
 * Enum with the different types for use as keys.  This enum acts a bridge between the Otel and Elasticsearch versions of each
 * of the instruments.
 */
public enum InstrumentType {
    DOUBLE_COUNTER(true),
    LONG_COUNTER(false),
    DOUBLE_UP_DOWN_COUNTER(true),
    LONG_UP_DOWN_COUNTER(false),
    DOUBLE_HISTOGRAM(true),
    LONG_HISTOGRAM(false),
    DOUBLE_GAUGE(true),
    LONG_GAUGE(false);

    public final boolean isDouble;
    public final boolean isLong;

    InstrumentType(boolean isDouble) {
        this.isDouble = isDouble;
        this.isLong = isDouble == false;
    }

    public static InstrumentType fromInstrument(Instrument instrument) {
        Objects.requireNonNull(instrument);
        if (instrument instanceof DoubleCounter) {
            return InstrumentType.DOUBLE_COUNTER;
        } else if (instrument instanceof LongCounter) {
            return InstrumentType.LONG_COUNTER;
        } else if (instrument instanceof DoubleUpDownCounter) {
            return InstrumentType.DOUBLE_UP_DOWN_COUNTER;
        } else if (instrument instanceof LongUpDownCounter) {
            return InstrumentType.LONG_UP_DOWN_COUNTER;
        } else if (instrument instanceof DoubleHistogram) {
            return InstrumentType.DOUBLE_HISTOGRAM;
        } else if (instrument instanceof LongHistogram) {
            return InstrumentType.LONG_HISTOGRAM;
        } else if (instrument instanceof DoubleGauge) {
            return InstrumentType.DOUBLE_GAUGE;
        } else if (instrument instanceof LongGauge) {
            return InstrumentType.LONG_GAUGE;
        } else {
            throw new IllegalArgumentException("unknown instrument [" + instrument.getClass().getName() + "]");
        }
    }
}
