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

public class NativePayloadSumScoreScript extends AbstractSearchScript {

    public static final String NATIVE_PAYLOAD_SUM_SCRIPT_SCORE = "native_payload_sum_script_score";
    String field = null;
    String[] terms = null;

    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new NativePayloadSumScoreScript(params);
        }
    }

    private NativePayloadSumScoreScript(Map<String, Object> params) {
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
            IndexFieldTerm indexFieldTerm = indexField.get(terms[i], IndexLookup.FLAG_PAYLOADS | IndexLookup.FLAG_CACHE);
            for (TermPosition pos : indexFieldTerm) {
                score += pos.payloadAsFloat(0);
            }
        }
        return score;
    }

}
