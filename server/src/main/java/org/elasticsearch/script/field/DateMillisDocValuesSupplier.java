/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DateMillisDocValuesSupplier extends DateDocValuesSupplier {

    public DateMillisDocValuesSupplier(SortedNumericDocValues docValues) {
        super(docValues);
    }

    @Override
    protected ZonedDateTime formatValue(long raw) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(raw), ZoneOffset.UTC);
    }
}
