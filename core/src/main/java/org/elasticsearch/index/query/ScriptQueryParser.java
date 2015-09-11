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

package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomAccessWeight;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class ScriptQueryParser implements QueryParser {

    public static final String NAME = "script";

    @Inject
    public ScriptQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[] { NAME };
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();

        XContentParser.Token token;

        // also, when caching, since its isCacheable is false, will result in loading all bit set...
        Script script = null;
        Map<String, Object> params = null;

        String queryName = null;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, parseContext.parseFieldMatcher());
                } else if ("params".equals(currentFieldName)) { // TODO remove in 3.0 (here to support old script APIs)
                    params = parser.map();
                } else {
                    throw new QueryParsingException(parseContext, "[script] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if (!scriptParameterParser.token(currentFieldName, token, parser, parseContext.parseFieldMatcher())) {
                    throw new QueryParsingException(parseContext, "[script] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (script == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                if (params == null) {
                    params = new HashMap<>();
                }
                script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
            }
        } else if (params != null) {
            throw new QueryParsingException(parseContext, "script params must be specified inside script object in a [script] filter");
        }

        if (script == null) {
            throw new QueryParsingException(parseContext, "script must be provided with a [script] filter");
        }

        Query query = new ScriptQuery(script, parseContext.scriptService(), parseContext.lookup());
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    static class ScriptQuery extends Query {

        private final Script script;

        private final SearchScript searchScript;

        public ScriptQuery(Script script, ScriptService scriptService, SearchLookup searchLookup) {
            this.script = script;
            this.searchScript = scriptService.search(searchLookup, script, ScriptContext.Standard.SEARCH);
        }

        @Override
        public String toString(String field) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("ScriptFilter(");
            buffer.append(script);
            buffer.append(")");
            return buffer.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!super.equals(obj))
                return false;
            ScriptQuery other = (ScriptQuery) obj;
            return Objects.equals(script, other.script);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + Objects.hashCode(script);
            return result;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
            return new RandomAccessWeight(this) {
                @Override
                protected Bits getMatchingDocs(final LeafReaderContext context) throws IOException {
                    final LeafSearchScript leafScript = searchScript.getLeafSearchScript(context);
                    return new Bits() {

                        @Override
                        public boolean get(int doc) {
                            leafScript.setDocument(doc);
                            Object val = leafScript.run();
                            if (val == null) {
                                return false;
                            }
                            if (val instanceof Boolean) {
                                return (Boolean) val;
                            }
                            if (val instanceof Number) {
                                return ((Number) val).longValue() != 0;
                            }
                            throw new IllegalArgumentException("Can't handle type [" + val + "] in script filter");
                        }

                        @Override
                        public int length() {
                            return context.reader().maxDoc();
                        }

                    };
                }
            };
        }
    }
}
