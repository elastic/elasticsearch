/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.session;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Configuration {

    private final String clusterName;
    private final String username;
    private final ZonedDateTime now;
    private final ZoneId zoneId;

    public Configuration(ZoneId zi, String username, String clusterName) {
        this.zoneId = zi.normalized();
        this.now = ZonedDateTime.now(zoneId);
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
