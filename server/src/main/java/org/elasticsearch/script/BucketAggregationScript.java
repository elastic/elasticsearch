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
 * A script used in bucket aggregations that returns a {@code double} value.
 */
public abstract class BucketAggregationScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("bucket_aggregation", Factory.class);

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    public BucketAggregationScript(Map<String, Object> params) {
        this.params = params;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract Number execute();

    public interface Factory extends ScriptFactory {
        BucketAggregationScript newInstance(Map<String, Object> params);
    }
}
