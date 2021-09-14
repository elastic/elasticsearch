/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.expertscript;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScoreScript.LeafFactory;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An example script plugin that adds a {@link ScriptEngine}
 * implementing expert scoring.
 */
public class ExpertScriptPlugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngine getScriptEngine(
        Settings settings,
        Collection<ScriptContext<?>> contexts
    ) {
        return new MyExpertScriptEngine();
    }

    /** An example {@link ScriptEngine} that uses Lucene segment details to
     *  implement pure document frequency scoring. */
    // tag::expert_engine
    private static class MyExpertScriptEngine implements ScriptEngine {
        @Override
        public String getType() {
            return "expert_scripts";
        }

        @Override
        public <T> T compile(
            String scriptName,
            String scriptSource,
            ScriptContext<T> context,
            Map<String, String> params
        ) {
            if (context.equals(ScoreScript.CONTEXT) == false) {
                throw new IllegalArgumentException(getType()
                        + " scripts cannot be used for context ["
                        + context.name + "]");
            }
            // we use the script "source" as the script identifier
            if ("pure_df".equals(scriptSource)) {
                ScoreScript.Factory factory = new PureDfFactory();
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("Unknown script name "
                    + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Set.of(ScoreScript.CONTEXT);
        }

        private static class PureDfFactory implements ScoreScript.Factory,
                                                      ScriptFactory {
            @Override
            public boolean isResultDeterministic() {
                // PureDfLeafFactory only uses deterministic APIs, this
                // implies the results are cacheable.
                return true;
            }

            @Override
            public LeafFactory newFactory(
                Map<String, Object> params,
                SearchLookup lookup
            ) {
                return new PureDfLeafFactory(params, lookup);
            }
        }

        private static class PureDfLeafFactory implements LeafFactory {
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            private final String field;
            private final String term;

            private PureDfLeafFactory(
                        Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException(
                            "Missing parameter [field]");
                }
                if (params.containsKey("term") == false) {
                    throw new IllegalArgumentException(
                            "Missing parameter [term]");
                }
                this.params = params;
                this.lookup = lookup;
                field = params.get("field").toString();
                term = params.get("term").toString();
            }

            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public ScoreScript newInstance(DocReader docReader)
                    throws IOException {
                DocValuesDocReader dvReader = ((DocValuesDocReader) docReader);
                PostingsEnum postings = dvReader.getLeafReaderContext()
                        .reader().postings(new Term(field, term));
                if (postings == null) {
                    /*
                     * the field and/or term don't exist in this segment,
                     * so always return 0
                     */
                    return new ScoreScript(params, lookup, docReader) {
                        @Override
                        public double execute(
                            ExplanationHolder explanation
                        ) {
                            return 0.0d;
                        }
                    };
                }
                return new ScoreScript(params, lookup, docReader) {
                    int currentDocid = -1;
                    @Override
                    public void setDocument(int docid) {
                        /*
                         * advance has undefined behavior calling with
                         * a docid <= its current docid
                         */
                        if (postings.docID() < docid) {
                            try {
                                postings.advance(docid);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                        currentDocid = docid;
                    }
                    @Override
                    public double execute(ExplanationHolder explanation) {
                        if (postings.docID() != currentDocid) {
                            /*
                             * advance moved past the current doc, so this
                             * doc has no occurrences of the term
                             */
                            return 0.0d;
                        }
                        try {
                            return postings.freq();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                };
            }
        }
    }
    // end::expert_engine
}
