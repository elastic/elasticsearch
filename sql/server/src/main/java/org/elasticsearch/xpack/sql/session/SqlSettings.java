/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.common.settings.Settings;

// Typed object holding properties for a given 
public class SqlSettings {

    public static final SqlSettings EMPTY = new SqlSettings(Settings.EMPTY);

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

    public boolean dateAsString() {
        return cfg.getAsBoolean(DATE_AS_STRING, false);
    }

    public SqlSettings dateAsString(boolean value) {
        return new SqlSettings(Settings.builder().put(cfg).put(DATE_AS_STRING, value).build());
    }


    private static final String DATE_AS_STRING = "sql.date_as_string";
}
