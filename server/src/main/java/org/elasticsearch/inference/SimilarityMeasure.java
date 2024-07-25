/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;

import java.util.EnumSet;
import java.util.Locale;

public enum SimilarityMeasure {
    COSINE,
    DOT_PRODUCT,
    L2_NORM;

    private static final EnumSet<SimilarityMeasure> BEFORE_L2_NORM_ENUMS = EnumSet.range(COSINE, DOT_PRODUCT);

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static SimilarityMeasure fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    /**
     * Returns a similarity measure that is known based on the transport version provided. If the similarity enum was not yet
     * introduced it will be defaulted to null.
     *
     * @param similarityMeasure the value to translate if necessary
     * @param version the version that dictates the translation
     * @return the similarity that is known to the version passed in
     */
    public static SimilarityMeasure translateSimilarity(SimilarityMeasure similarityMeasure, TransportVersion version) {
        if (version.before(TransportVersions.V_8_14_0) && BEFORE_L2_NORM_ENUMS.contains(similarityMeasure) == false) {
            return null;
        }

        return similarityMeasure;
    }
}
