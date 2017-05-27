/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.xpack.sql.util.ObjectUtils;

public class DateType extends AbstractDataType {

    public static final List<String> DEFAULT_FORMAT = Arrays.asList("strict_date_optional_time", "epoch_millis");
    public static final DateType DEFAULT = new DateType(true);

    private final List<String> formats;

    DateType(boolean docValues, String... formats) {
        super(JDBCType.TIMESTAMP_WITH_TIMEZONE, docValues);
        this.formats = ObjectUtils.isEmpty(formats) ? DEFAULT_FORMAT : Arrays.asList(formats);
    }

    @Override
    public String esName() {
        return "date";
    }

    @Override
    public int precision() {
        // same as Long
        // TODO: based this on format string
        return 19;
    }

    public List<String> formats() {
        return formats;
    }
}
