/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.session;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Configuration {

    private final String clusterName;
    private final String username;
    private final ZonedDateTime now;
    private final ZoneId zoneId;

    public Configuration(ZoneId zi, String username, String clusterName) {
        this.zoneId = zi.normalized();
        Clock clock = Clock.system(zoneId);
        this.now = ZonedDateTime.now(Clock.tick(clock, Duration.ofNanos(1)));
        this.username = username;
        this.clusterName = clusterName;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public ZonedDateTime now() {
        return now;
    }

    public String clusterName() {
        return clusterName;
    }

    public String username() {
        return username;
    }
}
