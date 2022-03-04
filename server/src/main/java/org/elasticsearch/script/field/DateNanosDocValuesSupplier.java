/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.time.DateUtils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DateNanosDocValuesSupplier extends DateDocValuesSupplier {

    public DateNanosDocValuesSupplier(SortedNumericDocValues docValues) {
        super(docValues);
    }

    @Override
    protected ZonedDateTime formatValue(long raw) {
        return ZonedDateTime.ofInstant(DateUtils.toInstant(raw), ZoneOffset.UTC);
    }
}
