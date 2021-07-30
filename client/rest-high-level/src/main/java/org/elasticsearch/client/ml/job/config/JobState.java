/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import java.util.Locale;

/**
 * Jobs whether running or complete are in one of these states.
 * When a job is created it is initialised in the state closed
 * i.e. it is not running.
 */
public enum JobState {

    CLOSING, CLOSED, OPENED, FAILED, OPENING;

    public static JobState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
