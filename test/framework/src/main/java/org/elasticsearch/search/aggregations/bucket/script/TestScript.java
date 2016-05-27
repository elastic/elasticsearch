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

package org.elasticsearch.search.aggregations.bucket.script;

import java.util.Objects;

import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ScriptHeuristic;

public abstract class TestScript implements ExecutableScript{

    ScriptHeuristic.LongAccessor _subset_freq;
    ScriptHeuristic.LongAccessor _subset_size;
    ScriptHeuristic.LongAccessor _superset_freq;
    ScriptHeuristic.LongAccessor _superset_size;

    protected TestScript() {
    }

    @Override
    public void setNextVar(String name, Object value) {
        if (name.equals("_subset_freq")) {
            _subset_freq = (ScriptHeuristic.LongAccessor)value;
        }
        if (name.equals("_subset_size")) {
            _subset_size = (ScriptHeuristic.LongAccessor)value;
        }
        if (name.equals("_superset_freq")) {
            _superset_freq = (ScriptHeuristic.LongAccessor)value;
        }
        if (name.equals("_superset_size")) {
            _superset_size = (ScriptHeuristic.LongAccessor)value;
        }
    }

    protected final void checkParams() {
        Objects.requireNonNull(_subset_freq, "_subset_freq");
        Objects.requireNonNull(_subset_size, "_subset_size");
        Objects.requireNonNull(_superset_freq, "_superset_freq");
        Objects.requireNonNull(_superset_size, "_superset_size");
    }

    @Override
    public Double unwrap(Object value) {
        return ((Number) value).doubleValue();
    }
}
