/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

/** Classifies the column types of an ES|QL response by their public REST names. */
final class EsqlColumnTypes {

    private EsqlColumnTypes() {}

    static boolean isDate(String outputType) {
        return "date".equals(outputType) || "date_nanos".equals(outputType);
    }
}
