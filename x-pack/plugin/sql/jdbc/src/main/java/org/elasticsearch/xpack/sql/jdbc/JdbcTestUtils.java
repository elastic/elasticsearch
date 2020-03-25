/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

final class JdbcTestUtils {

    private JdbcTestUtils() {}

    static ZonedDateTime nowWithMillisResolution(ZoneId zoneId) {
        Clock millisResolutionClock = Clock.tick(Clock.system(zoneId), Duration.ofMillis(1));
        return ZonedDateTime.now(millisResolutionClock);
    }
}
