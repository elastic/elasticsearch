/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

public final class RankFeatureQueryBuilders {
    private RankFeatureQueryBuilders() {}

    /**
     * Return a new {@link RankFeatureQueryBuilder} that will score documents as
     * {@code boost * S / (S + pivot)} where S is the value of the static feature.
     * @param fieldName   field that stores features
     * @param pivot       feature value that would give a score contribution equal to weight/2, must be in (0, +Infinity)
     */
    public static RankFeatureQueryBuilder saturation(String fieldName, float pivot) {
        return new RankFeatureQueryBuilder(fieldName, new RankFeatureQueryBuilder.ScoreFunction.Saturation(pivot));
    }

    /**
     * Same as {@link #saturation(String, float)} but a reasonably good default pivot value
     * is computed based on index statistics and is approximately equal to the geometric mean of all
     * values that exist in the index.
     * @param fieldName   field that stores features
     */
    public static RankFeatureQueryBuilder saturation(String fieldName) {
        return new RankFeatureQueryBuilder(fieldName, new RankFeatureQueryBuilder.ScoreFunction.Saturation());
    }

    /**
     * Return a new {@link RankFeatureQueryBuilder} that will score documents as
     * {@code boost * Math.log(scalingFactor + S)} where S is the value of the static feature.
     * @param fieldName     field that stores features
     * @param scalingFactor scaling factor applied before taking the logarithm, must be in [1, +Infinity)
     */
    public static RankFeatureQueryBuilder log(String fieldName, float scalingFactor) {
        return new RankFeatureQueryBuilder(fieldName, new RankFeatureQueryBuilder.ScoreFunction.Log(scalingFactor));
    }

    /**
     * Return a new {@link RankFeatureQueryBuilder} that will score documents as
     * {@code boost * S^a / (S^a + pivot^a)} where S is the value of the static feature.
     * @param fieldName   field that stores features
     * @param pivot       feature value that would give a score contribution equal to weight/2, must be in (0, +Infinity)
     * @param exp         exponent, higher values make the function grow slower before 'pivot' and faster after 'pivot',
     *                    must be in (0, +Infinity)
     */
    public static RankFeatureQueryBuilder sigmoid(String fieldName, float pivot, float exp) {
        return new RankFeatureQueryBuilder(fieldName, new RankFeatureQueryBuilder.ScoreFunction.Sigmoid(pivot, exp));
    }

    /**
     * Return a new {@link RankFeatureQueryBuilder} that will score documents as
     * {@code S)} where S is the indexed value of the static feature.
     * @param fieldName     field that stores features
     */
    public static RankFeatureQueryBuilder linear(String fieldName) {
        return new RankFeatureQueryBuilder(fieldName, new RankFeatureQueryBuilder.ScoreFunction.Linear());
    }

}
