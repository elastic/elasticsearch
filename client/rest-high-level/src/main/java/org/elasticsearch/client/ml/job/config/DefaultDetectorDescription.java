/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        return Strings.isNullOrEmpty(arg) == false;
    }
}
