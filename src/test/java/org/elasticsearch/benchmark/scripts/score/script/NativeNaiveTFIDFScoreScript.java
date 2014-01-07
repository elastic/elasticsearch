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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.IndexField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class NativeNaiveTFIDFScoreScript extends AbstractSearchScript {

    public static final String NATIVE_NAIVE_TFIDF_SCRIPT_SCORE = "native_naive_tfidf_script_score";
    String field = null;
    String[] terms = null;

    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new NativeNaiveTFIDFScoreScript(params);
        }
    }

    private NativeNaiveTFIDFScoreScript(Map<String, Object> params) {
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
            IndexFieldTerm indexFieldTerm = indexField.get(terms[i]);
            try {
                if (indexFieldTerm.tf() != 0) {
                    score += indexFieldTerm.tf() * indexField.docCount() / indexFieldTerm.df();
                }
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
        return score;
    }

}
