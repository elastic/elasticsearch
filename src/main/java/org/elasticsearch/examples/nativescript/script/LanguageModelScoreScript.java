package org.elasticsearch.examples.nativescript.script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

/**
 * Script that scores documents with a language model similarity with linear
 * interpolation, see Manning et al., "Information Retrieval", Chapter 12,
 * Equation 12.12 (link: http://nlp.stanford.edu/IR-book/) This implementation
 * only scores a list of terms on one field.
 */
public class LanguageModelScoreScript extends AbstractSearchScript {

    // the field containing the terms that should be scored, must be initialized
    // in constructor from parameters.
    String field;
    // name of the field that holds the word count of a field, see
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html)
    String docLengthField;
    // terms that are used for scoring
    ArrayList<String> terms;
    // lambda parameter
    float lambda;

    final static public String SCRIPT_NAME = "language_model_script_score";

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
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new LanguageModelScoreScript(params);
        }
    }

    /**
     * @param params
     *            terms that a scored are placed in this parameter. Initialize
     *            them here.
     */
    private LanguageModelScoreScript(Map<String, Object> params) {
        params.entrySet();
        // get the terms
        terms = (ArrayList<String>) params.get("terms");
        // get the field
        field = (String) params.get("field");
        // get the field holding the document length
        docLengthField = (String) params.get("word_count_field");
        // get lambda
        lambda = ((Double) params.get("lambda")).floatValue();
        if (field == null || terms == null || docLengthField == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field, terms or length field parameter missing!");
        }
    }

    @Override
    public Object run() {
        try {
            double score = 0.0;
            // first, get the ShardTerms object for the field.
            IndexField indexField = indexLookup().get(field);
            long T = indexField.sumttf();
            /*
             * document length cannot be obtained by the shardTerms, we use the
             * word_count field instead (link:
             * http://www.elasticsearch.org/guide
             * /en/elasticsearch/reference/current/mapping-core-types.html)
             */
            ScriptDocValues docValues = (ScriptDocValues) doc().get(docLengthField);
            if (docValues == null || !docValues.isEmpty()) {
                long L_d = ((ScriptDocValues.Longs) docValues).getValue();
                for (int i = 0; i < terms.size(); i++) {
                    // Now, get the ShardTerm object that can be used to access
                    // all
                    // the term statistics
                    IndexFieldTerm indexFieldTerm = indexField.get(terms.get(i));

                    /*
                     * compute M_c as ttf/T, see Manning et al.,
                     * "Information Retrieval", Chapter 12, Equation just before
                     * Equation 12.10 (link: http://nlp.stanford.edu/IR-book/)
                     */
                    long cf_t = indexFieldTerm.ttf();
                    double M_c = (double) cf_t / (double) T;
                    /*
                     * Compute M_d, see Manning et al., "Information Retrieval",
                     * Chapter 12, Equation just before Equation 12.9 (link:
                     * http://nlp.stanford.edu/IR-book/)
                     */
                    double M_d = (double) indexFieldTerm.tf() / (double) L_d;
                    /*
                     * compute score contribution for this term, but sum the log
                     * to avoid underflow, see Manning et al.,
                     * "Information Retrieval", Chapter 12, Equation 12.12
                     * (link: http://nlp.stanford.edu/IR-book/)
                     */
                    score += Math.log((1.0 - lambda) * M_c + lambda * M_d);

                }
                return score;
            } else {
                throw new ScriptException("Could not compute language model score, word count field missing.");
            }

        } catch (IOException ex) {
            throw new ScriptException("Could not compute language model score: ", ex);
        }
    }

}
