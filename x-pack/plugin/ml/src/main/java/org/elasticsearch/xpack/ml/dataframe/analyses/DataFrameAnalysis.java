/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.analyses;

import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Locale;
import java.util.Map;

public interface DataFrameAnalysis extends ToXContentObject {

    enum Type {
        OUTLIER_DETECTION;

        public static Type fromString(String value) {
            return Type.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    Type getType();

    interface Factory {

        /**
         * Creates a data frame analysis based on the specified map of maps config.
         *
         * @param config The configuration for the analysis
         *
         * <b>Note:</b> Implementations are responsible for removing the used configuration keys, so that after
         * creation it can be verified that all configurations settings have been used.
         */
        DataFrameAnalysis create(Map<String, Object> config);
    }
}
