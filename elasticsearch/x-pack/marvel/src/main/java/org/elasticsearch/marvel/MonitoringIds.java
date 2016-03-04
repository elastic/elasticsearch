/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import java.util.Locale;

public enum MonitoringIds {

    ES("es");

    private final String id;

    MonitoringIds(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public static MonitoringIds fromId(String id) {
        switch (id.toLowerCase(Locale.ROOT)) {
            case "es":
                return ES;
            default:
                throw new IllegalArgumentException("Unknown monitoring id [" + id + "]");
        }
    }
}
