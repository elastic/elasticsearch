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

package org.elasticsearch.example.expertscript;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 * An example script plugin that adds a {@link ScriptEngineService} implementing expert scoring.
 */
public class ExpertScriptPlugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngineService getScriptEngineService(Settings settings) {
        return new MyExpertScriptEngine();
    }

    /** An example {@link ScriptEngineService} that uses Lucene segment details to implement pure document frequency scoring. */
    // tag::expert_engine
    private static class MyExpertScriptEngine implements ScriptEngineService {
        @Override
        public String getType() {
            return "expert_scripts";
        }

        @Override
        public Function<Map<String,Object>,SearchScript> compile(String scriptName, String scriptSource, Map<String, String> params) {
            // we use the script "source" as the script identifier
            if ("pure_df".equals(scriptSource)) {
                return p -> new SearchScript() {
                    final String field;
                    final String term;
                    {
                        if (p.containsKey("field") == false) {
                            throw new IllegalArgumentException("Missing parameter [field]");
                        }
                        if (p.containsKey("term") == false) {
                            throw new IllegalArgumentException("Missing parameter [term]");
                        }
                        field = p.get("field").toString();
                        term = p.get("term").toString();
                    }

                    @Override
                    public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                        PostingsEnum postings = context.reader().postings(new Term(field, term));
                        if (postings == null) {
                            // the field and/or term don't exist in this segment, so always return 0
                            return () -> 0.0d;
                        }
                        return new LeafSearchScript() {
                            int currentDocid = -1;
                            @Override
                            public void setDocument(int docid) {
                                // advance has undefined behavior calling with a docid <= its current docid
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
                            public double runAsDouble() {
                                if (postings.docID() != currentDocid) {
                                    // advance moved past the current doc, so this doc has no occurrences of the term
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

                    @Override
                    public boolean needsScores() {
                        return false;
                    }
                };
            }
            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

        @Override
        @SuppressWarnings("unchecked")
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> params) {
          Function<Map<String,Object>,SearchScript> scriptFactory = (Function<Map<String,Object>,SearchScript>) compiledScript.compiled();
          return scriptFactory.apply(params);
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> params) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }

        @Override
        public void close() {}
    }
    // end::expert_engine
}
