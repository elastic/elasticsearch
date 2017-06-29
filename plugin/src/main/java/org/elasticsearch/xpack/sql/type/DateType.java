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
        /* Since we normalize timestamps to UTC for storage and do not keep
         * the origination zone information information we are technically
         * `TIMESTAMP WITHOUT TIME ZONE` or just `TIMESTAMP`, or, in Oracle
         * parlance, `TIMESTAMP WITH LOCAL TIME ZONE`.
         * `TIMESTAMP WITH TIME ZONE` implies that we store the original
         * time zone of the even. Confusingly, PostgreSQL's
         * `TIMESTAMP WITH TIME ZONE` type does not store original time zone,
         * unlike H2 and Oracle, *but* it is aware of the session's time zone
         * so it is preferred. But it is *weird*. As bad as it feels not to
         * be like PostgreSQL, we are going to not be like PostgreSQL here
         * and return TIMESTAMP so we more closely conform with H2 and
         * (shudder) Oracle. */
        super(JDBCType.TIMESTAMP, docValues);
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
