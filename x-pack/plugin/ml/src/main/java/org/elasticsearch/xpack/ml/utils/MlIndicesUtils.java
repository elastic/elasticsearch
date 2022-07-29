/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.action.support.IndicesOptions;

/**
 * Common index related operations that ML requires.
 */
public final class MlIndicesUtils {

    private MlIndicesUtils() {}

    public static IndicesOptions addIgnoreUnavailable(IndicesOptions indicesOptions) {
        return IndicesOptions.fromOptions(
            true,
            indicesOptions.allowNoIndices(),
            indicesOptions.expandWildcardsOpen(),
            indicesOptions.expandWildcardsClosed(),
            indicesOptions
        );
    }
}
