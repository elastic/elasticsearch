
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

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Map;

/**
 * A script used by the Ingest Script Processor.
 */
public abstract class IngestScript {

    public static final String[] PARAMETERS = { "ctx" };

    /** The context used to compile {@link IngestScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("ingest", Factory.class,
        200, TimeValue.timeValueMillis(0), new Tuple<>(375, TimeValue.timeValueMinutes(5)));

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public IngestScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute(Map<String, Object> ctx);

    public interface Factory {
        IngestScript newInstance(Map<String, Object> params);
    }
}
