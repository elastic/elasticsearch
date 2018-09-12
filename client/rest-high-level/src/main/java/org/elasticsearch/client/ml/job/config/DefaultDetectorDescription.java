/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.Strings;

public final class DefaultDetectorDescription {
    private static final String BY_TOKEN = " by ";
    private static final String OVER_TOKEN = " over ";

    private static final String USE_NULL_OPTION = " usenull=";
    private static final String PARTITION_FIELD_OPTION = " partitionfield=";
    private static final String EXCLUDE_FREQUENT_OPTION = " excludefrequent=";

    private DefaultDetectorDescription() {
    }

    /**
     * Returns the default description for the given {@code detector}
     *
     * @param detector the {@code Detector} for which a default description is requested
     * @return the default description
     */
    public static String of(Detector detector) {
        StringBuilder sb = new StringBuilder();
        appendOn(detector, sb);
        return sb.toString();
    }

    /**
     * Appends to the given {@code StringBuilder} the default description
     * for the given {@code detector}
     *
     * @param detector the {@code Detector} for which a default description is requested
     * @param sb       the {@code StringBuilder} to append to
     */
    public static void appendOn(Detector detector, StringBuilder sb) {
        if (isNotNullOrEmpty(detector.getFunction().getFullName())) {
            sb.append(detector.getFunction());
            if (isNotNullOrEmpty(detector.getFieldName())) {
                sb.append('(').append(quoteField(detector.getFieldName()))
                .append(')');
            }
        } else if (isNotNullOrEmpty(detector.getFieldName())) {
            sb.append(quoteField(detector.getFieldName()));
        }

        if (isNotNullOrEmpty(detector.getByFieldName())) {
            sb.append(BY_TOKEN).append(quoteField(detector.getByFieldName()));
        }

        if (isNotNullOrEmpty(detector.getOverFieldName())) {
            sb.append(OVER_TOKEN).append(quoteField(detector.getOverFieldName()));
        }

        if (detector.isUseNull()) {
            sb.append(USE_NULL_OPTION).append(detector.isUseNull());
        }

        if (isNotNullOrEmpty(detector.getPartitionFieldName())) {
            sb.append(PARTITION_FIELD_OPTION).append(quoteField(detector.getPartitionFieldName()));
        }

        if (detector.getExcludeFrequent() != null) {
            sb.append(EXCLUDE_FREQUENT_OPTION).append(detector.getExcludeFrequent());
        }
    }

    private static String quoteField(String field) {
        if (field.matches("\\w*")) {
            return field;
        } else {
            return "\"" + field.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
    }

    private static boolean isNotNullOrEmpty(String arg) {
        return !Strings.isNullOrEmpty(arg);
    }
}
