/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.watcher.WatcherException;

/**
 *
 */
public class Wid {

    private static final DateTimeFormatter formatter = ISODateTimeFormat.dateTime();

    private final String watchId;

    private final String value;

    public Wid(String watchId, long nonce, DateTime executionTime) {
        this.watchId = watchId;
        this.value = watchId + "_" + String.valueOf(nonce) + "#" +  formatter.print(executionTime);
    }

    public Wid(String value) {
        this.value = value;
        int index = value.lastIndexOf("_");
        if (index <= 0) {
            throw new WatcherException("invalid watcher execution id [{}]", value);
        }
        this.watchId = value.substring(0, index);
    }

    public String value() {
        return value;
    }

    public String watchId() {
        return watchId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Wid wid = (Wid) o;

        return value.equals(wid.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
