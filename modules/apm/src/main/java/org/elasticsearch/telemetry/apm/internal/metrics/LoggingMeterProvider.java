/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableDoubleUpDownCounter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;

import java.util.function.Consumer;

public class LoggingMeterProvider implements Meter {
    @Override
    public LongCounterBuilder counterBuilder(String name) {
        return new LoggingLongCounterBuilder(name);
    }

    @Override
    public LongUpDownCounterBuilder upDownCounterBuilder(String name) {
        return new LoggingLongUpDownCounterBuilder(name);
    }

    @Override
    public DoubleHistogramBuilder histogramBuilder(String name) {
        return new LoggingDoubleHistogramBuilder(name);
    }

    @Override
    public DoubleGaugeBuilder gaugeBuilder(String name) {
        return new LoggingDoubleGaugeBuilder(name);
    }

    static class LoggingDoubleCounterBuilder extends AbstractBuilder implements DoubleCounterBuilder {
        LoggingDoubleCounterBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public ObservableDoubleCounter buildWithCallback(Consumer<ObservableDoubleMeasurement> callback) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public DoubleCounter build() {
            return new LoggingInstrument("double-counter", name, description, unit);
        }

        @Override
        public ObservableDoubleMeasurement buildObserver() {
            return new LoggingInstrument("double-gauge", name, description, unit);
        }

        @Override
        public LoggingDoubleCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingDoubleCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }
    }

    static class LoggingLongCounterBuilder extends AbstractBuilder implements LongCounterBuilder {
        LoggingLongCounterBuilder(String name) {
            super(name);
        }

        @Override
        public DoubleCounterBuilder ofDoubles() {
            return new LoggingDoubleCounterBuilder(this);
        }

        @Override
        public ObservableLongCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public LongCounter build() {
            return new LoggingInstrument("long-counter", name, description, unit);
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            return new LoggingInstrument("long-gauge", name, description, unit);
        }

        @Override
        public LoggingLongCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingLongCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }
    }

    static class LoggingDoubleUpDownCounterBuilder extends AbstractBuilder implements DoubleUpDownCounterBuilder {

        LoggingDoubleUpDownCounterBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public DoubleUpDownCounter build() {
            return new LoggingInstrument("up-down-counter", name, description, unit);
        }

        @Override
        public ObservableDoubleUpDownCounter buildWithCallback(Consumer<ObservableDoubleMeasurement> callback) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public ObservableDoubleMeasurement buildObserver() {
            return new LoggingInstrument("gauge", name, description, unit);
        }

        @Override
        public LoggingDoubleUpDownCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingDoubleUpDownCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }
    }

    static class LoggingLongUpDownCounterBuilder extends AbstractBuilder implements LongUpDownCounterBuilder {

        LoggingLongUpDownCounterBuilder(String name) {
            super(name);
        }

        @Override
        public DoubleUpDownCounterBuilder ofDoubles() {
            return new LoggingDoubleUpDownCounterBuilder(this);
        }

        @Override
        public ObservableLongUpDownCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public LongUpDownCounter build() {
            return new LoggingInstrument("up-down-counter", name, description, unit);
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            return new LoggingInstrument("gauge", name, description, unit);
        }

        @Override
        public LoggingLongUpDownCounterBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingLongUpDownCounterBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }
    }

    static class LoggingDoubleHistogramBuilder extends AbstractBuilder implements DoubleHistogramBuilder {

        LoggingDoubleHistogramBuilder(String name) {
            super(name);
        }

        @Override
        public DoubleHistogram build() {
            return new LoggingInstrument("histogram", name, description, unit);
        }

        @Override
        public LongHistogramBuilder ofLongs() {
            return new LoggingLongHistogramBuilder(this);
        }

        @Override
        public DoubleHistogramBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public DoubleHistogramBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

    }

    static class LoggingLongHistogramBuilder extends AbstractBuilder implements LongHistogramBuilder {

        LoggingLongHistogramBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public LongHistogram build() {
            return new LoggingInstrument("histogram", name, description, unit);
        }

        @Override
        public LoggingLongHistogramBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingLongHistogramBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }
    }

    static class LoggingDoubleGaugeBuilder extends AbstractBuilder implements DoubleGaugeBuilder {
        LoggingDoubleGaugeBuilder(String name) {
            super(name);
        }

        @Override
        public LongGaugeBuilder ofLongs() {
            return new LoggingLongGaugeBuilder(this);
        }

        @Override
        public ObservableDoubleGauge buildWithCallback(Consumer<ObservableDoubleMeasurement> callback) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public ObservableDoubleMeasurement buildObserver() {
            return new LoggingInstrument("gauge", name, description, unit);
        }

        @Override
        public LoggingDoubleGaugeBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingDoubleGaugeBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }
    }

    static class LoggingLongGaugeBuilder extends AbstractBuilder implements LongGaugeBuilder {
        LoggingLongGaugeBuilder(AbstractBuilder other) {
            super(other);
        }

        @Override
        public ObservableLongGauge buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            throw new UnsupportedOperationException("unimplemented");
        }

        @Override
        public ObservableLongMeasurement buildObserver() {
            return new LoggingInstrument("gauge", name, description, unit);
        }

        @Override
        public LoggingLongGaugeBuilder setDescription(String description) {
            innerSetDescription(description);
            return this;
        }

        @Override
        public LoggingLongGaugeBuilder setUnit(String unit) {
            innerSetUnit(unit);
            return this;
        }

    }

    abstract static class AbstractBuilder {
        protected final String name;
        protected String description;
        protected String unit;

        AbstractBuilder(String name) {
            this.name = name;
        }

        AbstractBuilder(AbstractBuilder other) {
            this.name = other.name;
            this.description = other.description;
            this.unit = other.unit;
        }

        void innerSetDescription(String description) {
            this.description = description;
        }

        void innerSetUnit(String unit) {
            this.unit = unit;
        }
    }
}
