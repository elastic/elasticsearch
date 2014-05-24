package org.elasticsearch.examples.nativescript.script;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexLookup;
import org.elasticsearch.search.lookup.TermPosition;

/**
 * Script that scores documents by distance of two given terms in a text, see
 * Manning et al., "Information Retrieval", Chapter 2.4 (link:
 * http://nlp.stanford.edu/IR-book/) for more information on positional indexes.
 * Might be useful if you search for names and know first and last name.
 */
public class PhraseScoreScript extends AbstractSearchScript {

    // the field containing the terms that should be scored, must be initialized
    // in constructor from parameters.
    String field = null;
    // terms that are used for scoring
    ArrayList<String> terms = null;

    final static public String SCRIPT_NAME = "phrase_script_score";

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
            return new PhraseScoreScript(params);
        }
    }

    /**
     * @param params
     *            terms that a scored are placed in this parameter. Initialize
     *            them here.
     */
    private PhraseScoreScript(Map<String, Object> params) {
        params.entrySet();
        // get the terms
        terms = (ArrayList<String>) params.get("terms");
        // get the field
        field = (String) params.get("field");
        if (field == null || terms == null) {
            throw new ScriptException("cannot initialize " + SCRIPT_NAME + ": field or terms parameter missing!");
        }
        assert (terms.size() == 2);
    }

    @Override
    public Object run() {
        double score = 1.e10;
        // first, get the ShardTerms object for the field.
        IndexField indexField = this.indexLookup().get(field);
        // get the positions iterators
        Iterator<TermPosition> firstNameIter = indexField.get(terms.get(0), IndexLookup.FLAG_POSITIONS).iterator();
        Iterator<TermPosition> lastNameIter = indexField.get(terms.get(1), IndexLookup.FLAG_POSITIONS).iterator();
        int lastNamePos = -1;
        while (firstNameIter.hasNext()) {
            int firstNamePos = firstNameIter.next().position;
            while (lastNameIter.hasNext() && lastNamePos < firstNamePos) {
                lastNamePos = lastNameIter.next().position;
            }
            score = Math.min(score, lastNamePos - firstNamePos);
        }
        return 1.0 / (float) score;
    }

}
