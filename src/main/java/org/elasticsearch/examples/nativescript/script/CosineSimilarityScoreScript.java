package org.elasticsearch.examples.nativescript.script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.script.ScriptException;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

/**
 * Script that scores documents with cosine similarity, see Manning et al.,
 * "Information Retrieval", Chapter 6, Eq. 6.12 (link:
 * http://nlp.stanford.edu/IR-book/). This implementation only scores a list of
 * terms on one field.
 */
public class CosineSimilarityScoreScript extends AbstractSearchScript {

    // the field containing the terms that should be scored, must be initialized
    // in constructor from parameters.
    String field = null;
    // terms that are used for scoring, must be unique
    ArrayList<String> terms = null;
    // weights, in case we want to put emphasis on a specific term. In the most
    // simple case, 1.0 for every term.
    ArrayList<Double> weights = null;

    final static public String SCRIPT_NAME = "cosine_sim_script_score";

    /**
     * Factory that is registered in
     * {@link org.elasticsearch.examples.nativescript.plugin.NativeScriptExamplesPlugin#onModule(org.elasticsearch.script.ScriptModule)}
     * method when the plugin is loaded.
     */
    public static class Factory implements NativeScriptFactory {

        /**
         * This method is called for every search on every shard.
         * 
         * @param params
         *            list of script parameters passed with the query
         * @return new native script
         */
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new CosineSimilarityScoreScript(params);
        }
    }

    /**
     * @param params
     *            terms that a scored are placed in this parameter. Initialize
     *            them here.
     * @throws ScriptException 
     */
    private CosineSimilarityScoreScript(Map<String, Object> params) throws ScriptException {
        params.entrySet();
        // get the terms
        terms = (ArrayList<String>) params.get("terms");
        weights = (ArrayList<Double>) params.get("weights");
        // get the field
        field = (String) params.get("field");
        if (field == null || terms == null || weights == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field, terms or weights parameter missing!");
        }
        if (weights.size() != terms.size()) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": terms and weights array must have same length!");
        }
    }

    @Override
    public Object run() {
        try {
            float score = 0;
            // first, get the ShardTerms object for the field.
            IndexField indexField = this.indexLookup().get(field);
            double queryWeightSum = 0.0f;
            double docWeightSum = 0.0f;
            for (int i = 0; i < terms.size(); i++) {
                // Now, get the ShardTerm object that can be used to access all
                // the term statistics
                IndexFieldTerm indexTermField = indexField.get(terms.get(i));
                // compute the most naive tfidf and add to current score
                int df = (int) indexTermField.df();
                int tf = indexTermField.tf();
                if (df != 0 && tf != 0) {
                    double termscore = (double) tf * weights.get(i);
                    score += termscore;
                    docWeightSum += Math.pow(tf, 2.0);
                }
                queryWeightSum += Math.pow(weights.get(i), 2.0);
            }
            return score / (Math.sqrt(docWeightSum) * Math.sqrt(queryWeightSum));
        } catch (IOException ex) {
            throw new ScriptException("Could not compute cosine similarity: ", ex);
        }
    }

}
