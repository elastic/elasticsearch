/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * AbstractGaugeAdapter records the latest measurement for the given {@link Attributes} for each
 * time period.  This is a locking implementation.
 * T is the Otel instrument
 * N is the value of measurement
 */
public abstract class AbstractGaugeAdapter<T, N extends Number> extends AbstractInstrument<T> {

    private ConcurrentHashMap<Attributes, N> records = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock.ReadLock addRecordLock;
    private final ReentrantReadWriteLock.WriteLock popRecordsLock;

    public AbstractGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
        ReentrantReadWriteLock recordsLock = new ReentrantReadWriteLock();
        addRecordLock = recordsLock.readLock();
        popRecordsLock = recordsLock.writeLock();
    }

    protected ConcurrentHashMap<Attributes, N> popRecords() {
        ConcurrentHashMap<Attributes, N> currentRecords;
        ConcurrentHashMap<Attributes, N> newRecords = new ConcurrentHashMap<>();
        try {
            popRecordsLock.lock();
            currentRecords = records;
            records = newRecords;
        } finally {
            popRecordsLock.unlock();
        }
        return currentRecords;
    }

    protected void record(N value, Attributes attributes) {
        try {
            addRecordLock.lock();
            records.put(attributes, value);
        } finally {
            addRecordLock.unlock();
        }
    }
}
