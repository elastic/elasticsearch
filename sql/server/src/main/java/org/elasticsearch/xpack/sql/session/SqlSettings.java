/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.common.settings.Settings;
import org.joda.time.DateTimeZone;

// Typed object holding properties for a given 
public class SqlSettings {

    public static final SqlSettings EMPTY = new SqlSettings(Settings.EMPTY);

    public static final String TIMEZONE_ID = "sql.timeZoneId";
    public static final String TIMEZONE_ID_DEFAULT = null;

    public static final String PAGE_SIZE = "sql.fetch.size";
    public static final int PAGE_SIZE_DEFAULT = 100;

    private final Settings cfg;

    public SqlSettings(Settings cfg) {
        this.cfg = cfg;
    }

    public Settings cfg() {
        return cfg;
    }

    @Override
    public String toString() {
        return cfg.toDelimitedString(',');
    }

    public String timeZoneId() {
        return cfg.get(TIMEZONE_ID, TIMEZONE_ID_DEFAULT);
    }

    public DateTimeZone timeZone() {
        return DateTimeZone.forID(timeZoneId());
    }

    public int pageSize() {
        return cfg.getAsInt(PAGE_SIZE, 100);
    }
}
