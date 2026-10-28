/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.action.support.IndicesOptions;

/**
 * Context object threaded through transform XContent parsing to carry
 * cross-project configuration that affects how indices options are resolved.
 */
public final class TransformParsingContext {

    private final boolean crossProjectEnabled;

    public TransformParsingContext(boolean crossProjectEnabled) {
        this.crossProjectEnabled = crossProjectEnabled;
    }

    public IndicesOptions getDefaultIndicesOptions() {
        // TODO allow IndicesOptions to be configured by the user
        // https://github.com/elastic/elasticsearch/issues/143613
        return crossProjectEnabled ? IndicesOptions.CPS_LENIENT_EXPAND_OPEN : IndicesOptions.LENIENT_EXPAND_OPEN;
    }
}
