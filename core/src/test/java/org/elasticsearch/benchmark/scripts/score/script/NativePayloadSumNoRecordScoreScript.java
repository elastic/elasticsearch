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

package org.elasticsearch.benchmark.scripts.score.script;

import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexLookup;
import org.elasticsearch.search.lookup.TermPosition;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.ArrayList;
import java.util.Map;

public class NativePayloadSumNoRecordScoreScript extends AbstractSearchScript {

    public static final String NATIVE_PAYLOAD_SUM_NO_RECORD_SCRIPT_SCORE = "native_payload_sum_no_record_script_score";
    String field = null;
    String[] terms = null;

    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new NativePayloadSumNoRecordScoreScript(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }

    private NativePayloadSumNoRecordScoreScript(Map<String, Object> params) {
        params.entrySet();
        terms = new String[params.size()];
        field = params.keySet().iterator().next();
        Object o = params.get(field);
        ArrayList<String> arrayList = (ArrayList<String>) o;
        terms = arrayList.toArray(new String[arrayList.size()]);

    }

    @Override
    public Object run() {
        float score = 0;
        IndexField indexField = indexLookup().get(field);
        for (int i = 0; i < terms.length; i++) {
            IndexFieldTerm indexFieldTerm = indexField.get(terms[i], IndexLookup.FLAG_PAYLOADS);
            for (TermPosition pos : indexFieldTerm) {
                score += pos.payloadAsFloat(0);
            }
        }
        return score;
    }

}
