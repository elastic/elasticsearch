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
