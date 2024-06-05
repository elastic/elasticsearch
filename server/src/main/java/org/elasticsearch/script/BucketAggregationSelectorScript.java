/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;

/**
 * A script used in bucket aggregations that returns a {@code boolean} value.
 */
public abstract class BucketAggregationSelectorScript {  // TODO move to the aggregation module

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("aggregation_selector", Factory.class);

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    public BucketAggregationSelectorScript(Map<String, Object> params) {
        this.params = params;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract boolean execute();

    public interface Factory {
        BucketAggregationSelectorScript newInstance(Map<String, Object> params);
    }
}
