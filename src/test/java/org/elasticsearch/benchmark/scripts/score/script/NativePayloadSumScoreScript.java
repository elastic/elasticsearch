package org.elasticsearch.benchmark.scripts.score.script;

import org.elasticsearch.search.lookup.ScriptTerm;
import org.elasticsearch.search.lookup.ScriptTerms;
import org.elasticsearch.search.lookup.ShardTermsLookup;
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
        ScriptTerms scriptTerms = shardTerms().get(field);
        for (int i = 0; i < terms.length; i++) {
            ScriptTerm scriptTerm = scriptTerms.get(terms[i], ShardTermsLookup.FLAG_PAYLOADS | ShardTermsLookup.FLAG_CACHE);
            for (TermPosition pos : scriptTerm) {
                score += pos.payloadAsFloat(0);
            }
        }
        return score;
    }

}
