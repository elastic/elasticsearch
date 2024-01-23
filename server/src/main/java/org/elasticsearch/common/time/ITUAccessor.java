/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import com.ethlo.time.DateTime;
import com.ethlo.time.TimezoneOffset;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.UnsupportedTemporalTypeException;

class ITUAccessor implements TemporalAccessor {

    private final DateTime dateTime;

    ITUAccessor(DateTime dateTime) {
        this.dateTime = dateTime;
    }

    @Override
    public boolean isSupported(TemporalField field) {
        try {
            return field.equals(ChronoField.OFFSET_SECONDS) || field.equals(ChronoField.NANO_OF_SECOND) || dateTime.isSupported(field);
        } catch (UnsupportedTemporalTypeException e) {
            return false;
        }
    }

    @Override
    public long getLong(TemporalField field) {
        if (field.equals(ChronoField.OFFSET_SECONDS)) {
            return dateTime.getOffset().map(TimezoneOffset::getTotalSeconds).orElse(0).longValue();
        } else if (field.equals(ChronoField.NANO_OF_SECOND)) {
            return dateTime.getNano();
        } else {
            return dateTime.getLong(field);
        }
    }
}
