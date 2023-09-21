/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.context.Context;

import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Objects;
import java.util.stream.Collectors;

public class LoggingInstrument
    implements
        DoubleCounter,
        DoubleUpDownCounter,
        ObservableDoubleMeasurement,
        DoubleHistogram,
        LongCounter,
        LongUpDownCounter,
        ObservableLongMeasurement,
        LongHistogram {
    private static final Logger log = LogManager.getLogger(LoggingInstrument.class);
    private final String type;
    private final String name;
    private final String description;
    private final String unit;

    private final String prefix;

    LoggingInstrument(String type, String name, String description, String unit) {
        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.unit = Objects.requireNonNull(unit);
        this.prefix = Strings.format("[%s:%s][%s] ", type, name, unit);
    }

    @Override
    public void add(double value) {
        log.info(action("add", value));
    }

    @Override
    public void add(double value, Attributes attributes) {
        log.info(action("add", value, attributes));
    }

    @Override
    public void add(double value, Attributes attributes, Context context) {
        log.info(action("add-context", value));
    }

    @Override
    public void add(long value, Attributes attributes, Context context) {
        log.info(action("add-context", value));
    }

    @Override
    public void record(double value) {
        log.info(action("record", value));
    }

    @Override
    public void record(double value, Attributes attributes) {
        log.info(action("record", value, attributes));
    }

    @Override
    public void record(double value, Attributes attributes, Context context) {
        log.info(action("record-context", value, attributes));
    }

    @Override
    public void add(long value) {
        log.info(action("add", value));
    }

    @Override
    public void add(long value, Attributes attributes) {
        log.info(action("add", value, attributes));
    }

    @Override
    public void record(long value) {
        log.info(action("record", value));
    }

    @Override
    public void record(long value, Attributes attributes) {
        log.info(action("record", value, attributes));
    }

    @Override
    public void record(long value, Attributes attributes, Context context) {
        log.info(action("record-context", value, attributes));
    }

    private String action(String action) {
        return Strings.format("%s [%s] [%s]", prefix, action);
    }

    private String action(String action, Attributes attributes) {
        return Strings.format("%s [%s] [%s] [%s]", prefix, attributeString(attributes), action);
    }

    private String action(String action, long value) {
        return Strings.format("%s [%s] [%d]", prefix, action, value);
    }

    private String action(String action, long value, Attributes attributes) {
        return Strings.format("%s [%s] [%s] %d", prefix, action, attributeString(attributes), value);
    }

    private String action(String action, double value) {
        return Strings.format("%s [%s] [%d]", prefix, action, value);
    }

    private String action(String action, double value, Attributes attributes) {
        return Strings.format("%s [%s] [%s] %d", prefix, action, attributeString(attributes), value);
    }

    String attributeString(Attributes attributes) {
        if (attributes == null) {
            return "null";
        }
        return attributes.asMap().keySet().stream().sorted().map(k -> k + "=" + attributes.get(k)).collect(Collectors.joining(","));
    }
}
