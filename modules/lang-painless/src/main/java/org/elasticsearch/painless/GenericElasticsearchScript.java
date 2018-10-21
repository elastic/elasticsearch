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

package org.elasticsearch.painless;

import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.util.Map;

/**
 * Generic script interface that Painless implements for all Elasticsearch scripts.
 */
public abstract class GenericElasticsearchScript {

    public GenericElasticsearchScript() {}

    public static final String[] PARAMETERS = new String[] {"params", "_score", "doc", "_value", "ctx"};
    public abstract Object execute(
        Map<String, Object> params, double _score, Map<String, ScriptDocValues<?>> doc, Object _value, Map<?, ?> ctx);

    public abstract boolean needs_score();
    public abstract boolean needsCtx();
}
