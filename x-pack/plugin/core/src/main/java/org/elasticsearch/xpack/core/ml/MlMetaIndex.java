/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

public final class MlMetaIndex {

    private static final String INDEX_NAME = ".ml-meta";

    /**
     * Where to store the ml info in Elasticsearch - must match what's
     * expected by kibana/engineAPI/app/directives/mlLogUsage.js
     *
     * @return The index name
     */
    public static String indexName() {
        return INDEX_NAME;
    }

    private MlMetaIndex() {}
}
