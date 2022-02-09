/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;

/**
 * A set of static factory methods for {@link SortBuilder}s.
 *
 *
 */
public class SortBuilders {

    /**
     * Constructs a new score sort.
     */
    public static ScoreSortBuilder scoreSort() {
        return new ScoreSortBuilder();
    }

    /**
     * Constructs a new field based sort.
     *
     * @param field The field name.
     */
    public static FieldSortBuilder fieldSort(String field) {
        return new FieldSortBuilder(field);
    }

    /**
     * Constructs a sort tiebreaker that can be used within a point in time reader {@link PointInTimeBuilder}.
     */
    public static FieldSortBuilder pitTiebreaker() {
        return new FieldSortBuilder(FieldSortBuilder.SHARD_DOC_FIELD_NAME);
    }

    /**
     * Constructs a new script based sort.
     *
     * @param script The script to use.
     * @param type   The type, can either be "string" or "number".
     */
    public static ScriptSortBuilder scriptSort(Script script, ScriptSortType type) {
        return new ScriptSortBuilder(script, type);
    }

    /**
     * A geo distance based sort.
     *
     * @param fieldName The geo point like field name.
     * @param lat Latitude of the point to create the range distance facets from.
     * @param lon Longitude of the point to create the range distance facets from.
     *
     */
    public static GeoDistanceSortBuilder geoDistanceSort(String fieldName, double lat, double lon) {
        return new GeoDistanceSortBuilder(fieldName, lat, lon);
    }

    /**
     * Constructs a new distance based sort on a geo point like field.
     *
     * @param fieldName The geo point like field name.
     * @param points The points to create the range distance facets from.
     */
    public static GeoDistanceSortBuilder geoDistanceSort(String fieldName, GeoPoint... points) {
        return new GeoDistanceSortBuilder(fieldName, points);
    }

    /**
     * Constructs a new distance based sort on a geo point like field.
     *
     * @param fieldName The geo point like field name.
     * @param geohashes The points to create the range distance facets from.
     */
    public static GeoDistanceSortBuilder geoDistanceSort(String fieldName, String... geohashes) {
        return new GeoDistanceSortBuilder(fieldName, geohashes);
    }
}
