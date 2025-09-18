/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * Class holding all the flags that can be used to change behavior for certain features in ESQL.
 * The flags are backed by {@link Setting}s so they can be dynamically changed.
 * When adding a new flag, make sure to add it to {@link #ALL_ESQL_FLAGS_SETTINGS}
 * so it gets registered and unit tests can pass.
 */
public class EsqlFlags {
    public static final Setting<Boolean> ESQL_STRING_LIKE_ON_INDEX = Setting.boolSetting(
        "esql.query.string_like_on_index",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The maximum number of rounding points to push down to Lucene for the {@code roundTo} function at cluster level.
     * {@code ReplaceRoundToWithQueryAndTags} checks this threshold before rewriting {@code RoundTo} to range queries.
     *
     * There is also a query level ROUNDTO_PUSHDOWN_THRESHOLD defined in {@code QueryPragmas}.
     * The cluster level threshold defaults to 127, it is the same as the maximum number of buckets used in {@code Rounding}.
     * The query level threshold defaults to -1, which means this query level setting is not set and cluster level upper limit will be used.
     * If query level threshold is set to greater than or equals to 0, the query level threshold will be used, and it overrides the cluster
     * level threshold.
     *
     * If the cluster level threshold is set to -1 or 0, no {@code RoundTo} pushdown will be performed, query level threshold is not set to
     * -1 or 0.
     */
    public static final Setting<Integer> ESQL_ROUNDTO_PUSHDOWN_THRESHOLD = Setting.intSetting(
        "esql.query.roundto_pushdown_threshold",
        127,
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // this is only used for testing purposes right now
    public static List<Setting<?>> ALL_ESQL_FLAGS_SETTINGS = List.of(ESQL_STRING_LIKE_ON_INDEX, ESQL_ROUNDTO_PUSHDOWN_THRESHOLD);

    private final boolean stringLikeOnIndex;

    private final int roundToPushdownThreshold;

    /**
     * Constructor for tests.
     */
    public EsqlFlags(boolean stringLikeOnIndex) {
        this.stringLikeOnIndex = stringLikeOnIndex;
        this.roundToPushdownThreshold = ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY);
    }

    /**
     * Constructor for tests.
     */
    public EsqlFlags(int roundToPushdownThreshold) {
        this.stringLikeOnIndex = ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY);
        this.roundToPushdownThreshold = roundToPushdownThreshold;
    }

    /**
     * Constructor for tests.
     */
    public EsqlFlags(boolean stringLikeOnIndex, int roundToPushdownThreshold) {
        this.stringLikeOnIndex = stringLikeOnIndex;
        this.roundToPushdownThreshold = roundToPushdownThreshold;
    }

    public EsqlFlags(ClusterSettings settings) {
        this.stringLikeOnIndex = settings.get(ESQL_STRING_LIKE_ON_INDEX);
        this.roundToPushdownThreshold = settings.get(ESQL_ROUNDTO_PUSHDOWN_THRESHOLD);
    }

    public boolean stringLikeOnIndex() {
        return stringLikeOnIndex;
    }

    public int roundToPushdownThreshold() {
        return roundToPushdownThreshold;
    }
}
