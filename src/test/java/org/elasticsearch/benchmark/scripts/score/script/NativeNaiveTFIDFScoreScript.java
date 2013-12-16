package org.elasticsearch.benchmark.scripts.score.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.ScriptTerm;
import org.elasticsearch.search.lookup.ScriptTerms;

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
        ScriptTerms scriptTerms = shardTerms().get(field);
        for (int i = 0; i < terms.length; i++) {
            ScriptTerm scriptTerm = scriptTerms.get(terms[i]);
            try {
                if (scriptTerm.tf() != 0) {
                    score += scriptTerm.tf() * scriptTerms.docCount() / scriptTerm.df();
                }
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
        return score;
    }

}
