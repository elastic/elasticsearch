/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.session;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Configuration {

    protected final String clusterName;
    protected final String username;
    protected final ZonedDateTime now;
    protected final ZoneId zoneId;

    public Configuration(ZoneId zi, String username, String clusterName) {
        this(zi, null, username, clusterName);
    }

    protected Configuration(ZoneId zi, Instant now, String username, String clusterName) {
        this.zoneId = zi.normalized();
        this.now = now != null ? now.atZone(zi) : ZonedDateTime.now(Clock.tick(Clock.system(zoneId), Duration.ofNanos(1)));
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Configuration that = (Configuration) o;
        return Objects.equals(zoneId, that.zoneId)
            && Objects.equals(now, that.now)
            && Objects.equals(username, that.username)
            && Objects.equals(clusterName, that.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId, now, username, clusterName);
    }
}
