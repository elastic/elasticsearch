/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import java.util.Locale;

public enum MonitoredSystem {

    ES("es"),
    KIBANA("kibana"),
    LOGSTASH("logstash"),
    BEATS("beats");

    private final String system;

    MonitoredSystem(String system) {
        this.system = system;
    }

    public String getSystem() {
        return system;
    }

    public static MonitoredSystem fromSystem(String system) {
        switch (system.toLowerCase(Locale.ROOT)) {
            case "es":
                return ES;
            case "kibana":
                return KIBANA;
            case "logstash":
                return LOGSTASH;
            case "beats":
                return BEATS;
            default:
                throw new IllegalArgumentException("Unknown monitoring system [" + system + "]");
        }
    }
}
