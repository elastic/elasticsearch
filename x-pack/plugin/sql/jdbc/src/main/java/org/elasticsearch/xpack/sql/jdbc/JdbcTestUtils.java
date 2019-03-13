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
