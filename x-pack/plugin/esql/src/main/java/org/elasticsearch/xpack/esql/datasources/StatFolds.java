/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.MAX_SUFFIX;
import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.MIN_SUFFIX;
import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.NULL_COUNT_SUFFIX;
import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.SIZE_BYTES_SUFFIX;
import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.STATS_ROW_COUNT;
import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.STATS_SIZE_BYTES;
import static org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.VALUE_COUNT_SUFFIX;

/**
 * Maps a flat {@code _stats.*} statistic key to its {@link StatFold} law, so a statistics combine folds
 * each key by its own rule instead of overwriting.
 *
 * <p>This is what makes the combine clobber-proof: a site merging statistics looks up
 * {@link #foldFor} per key and folds — different statistics live under different keys and cannot
 * collide; the same statistic folds by its law and cannot overwrite. The fold law lives once, here +
 * {@link StatFold}, never re-implemented at a combine site.
 */
final class StatFolds {

    private StatFolds() {}

    /** The fold law for a flat statistic key, or {@code null} if the key is not a foldable statistic. */
    static StatFold foldFor(String key) {
        if (key.endsWith(MIN_SUFFIX)) {
            return StatFold.MIN;
        }
        if (key.endsWith(MAX_SUFFIX)) {
            return StatFold.MAX;
        }
        if (key.equals(STATS_ROW_COUNT)
            || key.equals(STATS_SIZE_BYTES)
            || key.endsWith(NULL_COUNT_SUFFIX)
            || key.endsWith(VALUE_COUNT_SUFFIX)
            || key.endsWith(SIZE_BYTES_SUFFIX)) {
            return StatFold.SUM;
        }
        return null;
    }
}
