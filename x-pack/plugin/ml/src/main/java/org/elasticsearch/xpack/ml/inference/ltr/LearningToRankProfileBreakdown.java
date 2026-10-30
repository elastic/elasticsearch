/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;

import java.util.Locale;

/**
 * Time spent in each step of the {@code learning_to_rank} rescorer, reported in the {@code breakdown} of its search profile. The steps
 * run back to back, so together they account for nearly all of the rescorer's time.
 */
final class LearningToRankProfileBreakdown extends AbstractProfileBreakdown<LearningToRankProfileBreakdown.TimingType> {

    enum TimingType {
        /**
         * Turning the feature queries into weights, once per rescore.
         */
        BUILD_FEATURE_EXTRACTORS,
        /**
         * Extracting the features of each rescored document, including running the feature queries.
         */
        FEATURE_EXTRACTION,
        /**
         * Running the model on the features of each rescored document.
         */
        INFERENCE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    LearningToRankProfileBreakdown() {
        super(TimingType.class);
    }
}
