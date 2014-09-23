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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

public class NativeSignificanceScoreScriptNoParams implements ExecutableScript {

    public static final String NATIVE_SIGNIFICANCE_SCORE_SCRIPT_NO_PARAMS = "native_significance_score_script_no_params";
    double _subset_freq = 0;
    double _subset_size = 0;
    double _superset_freq = 0;
    double _superset_size = 0;

    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new NativeSignificanceScoreScriptNoParams(params);
        }
    }

    private NativeSignificanceScoreScriptNoParams(Map<String, Object> params) {
    }

    @Override
    public void setNextVar(String name, Object value) {
        if (name.equals("_subset_freq")) {
            _subset_freq = unwrap(value);
        }
        if (name.equals("_subset_size")) {
            _subset_size = unwrap(value);
        }
        if (name.equals("_superset_freq")) {
            _superset_freq = unwrap(value);
        }
        if (name.equals("_superset_size")) {
            _superset_size = unwrap(value);
        }
    }

    @Override
    public Object run() {
        return _subset_freq + _subset_size + _superset_freq + _superset_size;
    }

    @Override
    public Double unwrap(Object value) {
        return ((Number) value).doubleValue();
    }

}
