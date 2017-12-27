/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

public final class MLMetadataField {

    public static final String TYPE = "ml";

    private MLMetadataField() {}

    /**
     * Namespaces the task ids for datafeeds.
     * A job id can be used as a datafeed id, because they are stored separately in cluster state.
     */
    public static String datafeedTaskId(String datafeedId) {
        return "datafeed-" + datafeedId;
    }
}
