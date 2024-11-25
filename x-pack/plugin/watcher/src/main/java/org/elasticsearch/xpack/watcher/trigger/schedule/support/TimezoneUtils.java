/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

/**
 * Utility class for dealing with Timezone related operations.
 */
public class TimezoneUtils {

    private static final Map<String, ZoneId> caseInsensitiveTZLookup;

    static {
        caseInsensitiveTZLookup = ZoneId.getAvailableZoneIds()
            .stream()
            .collect(toMap(zoneId -> zoneId.toLowerCase(Locale.ROOT), ZoneId::of));
    }

    /**
     * Parses a timezone string into a {@link ZoneId} object. The timezone string can be a valid timezone ID, or a
     * timezone offset string and is case-insensitive.
     *
     * @param timezoneString The timezone string to parse
     * @return The parsed {@link ZoneId} object
     * @throws DateTimeException If the timezone string is not a valid timezone ID or offset
     */
    public static ZoneId parse(String timezoneString) throws DateTimeException {
        try {
            return ZoneId.of(timezoneString);
        } catch (DateTimeException e) {
            ZoneId timeZone = caseInsensitiveTZLookup.get(timezoneString.toLowerCase(Locale.ROOT));
            if (timeZone != null) {
                return timeZone;
            }
            try {
                return ZoneId.of(timezoneString.toUpperCase(Locale.ROOT));
            } catch (DateTimeException ignored) {
                // ignore
            }
            throw e;
        }
    }

}
