package org.elasticsearch.benchmark.scripts.score.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.termstatistics.ScriptField;
import org.elasticsearch.search.lookup.termstatistics.ScriptTerm;

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
        ScriptField fi = termStatistics().get(field);
        for (int i = 0; i < terms.length; i++) {
            ScriptTerm ti = fi.get(terms[i]);
            try {
                score += ti.freq() * fi.docCount() / ti.df();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
        return score;
    }

}
