/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.monitoring;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;

public enum MonitoredSystem {

    ES("es"),
    KIBANA("kibana"),
    LOGSTASH("logstash"),
    BEATS("beats"),
    UNKNOWN("unknown");

    private final String system;

    MonitoredSystem(String system) {
        this.system = system;
    }

    public String getSystem() {
        return system;
    }

    public static MonitoredSystem fromSystem(String system) {
        return switch (system.toLowerCase(Locale.ROOT)) {
            case "es" -> ES;
            case "kibana" -> KIBANA;
            case "logstash" -> LOGSTASH;
            case "beats" -> BEATS;
            default ->
                // Return an "unknown" monitored system
                // that can easily be filtered out if
                // a node receives documents for a new
                // system it does not know yet
                UNKNOWN;
        };
    }

    /**
     * Get all {@code MonitoredSystem}s except {@linkplain MonitoredSystem#UNKNOWN UNKNOWN}.
     *
     * @return Never {@code null}. A filtered {@code Stream} that removes the {@code UNKNOWN} {@code MonitoredSystem}.
     */
    public static Stream<MonitoredSystem> allSystems() {
        return Arrays.stream(MonitoredSystem.values()).filter(s -> s != MonitoredSystem.UNKNOWN);
    }
}
