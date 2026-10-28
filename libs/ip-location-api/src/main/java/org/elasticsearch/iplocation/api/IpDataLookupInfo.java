/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

import java.util.SequencedMap;

/**
 * Metadata about an IP data lookup, available immediately without waiting
 * for database download. Provides ordered field information for ES|QL integration.
 */
public interface IpDataLookupInfo {

    /**
     * Returns an ordered map of all field names to their types for this database
     * (e.g., "city_name" -> String.class, "location" -> Object.class).
     * <p>
     * Note: the {@code location} field maps to {@code Object.class} (not {@code GeoPoint.class})
     * because {@link DatabaseProperty#LOCATION} uses {@code Object.class} as its field type.
     * Consumers that need to recognize geo-point fields should check via
     * {@link DatabaseProperty#LOCATION} enum identity rather than the {@code Class<?>} value.
     */
    SequencedMap<String, Class<?>> getFields();

    /**
     * Returns an ordered map of the <em>default</em> field names to their types for this database.
     * This is the curated subset of {@link #getFields()} that is used when no explicit properties
     * are specified (e.g., City defaults to country_iso_code, country_name, continent_name,
     * region_iso_code, region_name, city_name, location).
     */
    SequencedMap<String, Class<?>> getDefaultFields();

    /**
     * Returns the database type string (e.g., "GeoLite2-City").
     */
    String getDatabaseType();
}
