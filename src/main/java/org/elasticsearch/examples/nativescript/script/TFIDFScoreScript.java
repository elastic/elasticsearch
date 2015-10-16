/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.examples.nativescript.script;

import java.io.IOException;
import java.util.List;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;

/**
 * Script that scores documents as sum_t(tf_t * (#docs+2)/(df_t+1)), which
 * equals ntn in SMART notation, see Manning et al., "Information Retrieval",
 * Chapter 6, Figure 6.15 (link: http://nlp.stanford.edu/IR-book/) This
 * implementation only scores a list of terms on one field.
 */
public class TFIDFScoreScript extends AbstractSearchScript {

    // the field containing the terms that should be scored, must be initialized
    // in constructor from parameters.
    String field = null;
    // terms that are used for scoring
    List<String> terms = null;

    final static public String SCRIPT_NAME = "tfidf_script_score";

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
            return new TFIDFScoreScript(params);
        }

        /**
         * Indicates if document scores may be needed by the produced scripts.
         *
         * @return {@code true} if scores are needed.
         */
        @Override
        public boolean needsScores() {
            return false;
        }
    }

    /**
     * @param params
     *            terms that a scored are placed in this parameter. Initialize
     *            them here.
     */
    @SuppressWarnings("unchecked")
    private TFIDFScoreScript(Map<String, Object> params) {
        params.entrySet();
        // get the terms
        terms = (List<String>) params.get("terms");
        // get the field
        field = (String) params.get("field");
        if (field == null || terms == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field or terms parameter missing!");
        }
    }

    @Override
    public Object run() {
        try {
            float score = 0;
            // first, get the IndexField object for the field.
            IndexField indexField = indexLookup().get(field);
            for (int i = 0; i < terms.size(); i++) {
                // Now, get the IndexFieldTerm object that can be used to access all
                // the term statistics
                IndexFieldTerm indexFieldTerm = indexField.get(terms.get(i));
                // compute the most naive tfidf and add to current score
                int df = (int) indexFieldTerm.df();
                int tf = indexFieldTerm.tf();
                if (df != 0 && tf != 0) {
                    score += (float) indexFieldTerm.tf() * Math.log(((float) indexField.docCount() + 2.0) / ((float) df + 1.0));
                }
            }
            return score;
        } catch (IOException ex) {
            throw new ScriptException("Could not compute tfidf: ", ex);
        }
    }

}
