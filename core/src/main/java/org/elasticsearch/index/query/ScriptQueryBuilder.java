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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class ScriptQueryBuilder extends AbstractQueryBuilder<ScriptQueryBuilder> {

    public static final String NAME = "script";

    static final ScriptQueryBuilder PROTOTYPE = new ScriptQueryBuilder(new Script(""));

    private final Script script;

    public ScriptQueryBuilder(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("script cannot be null");
        }
        this.script = script;
    }

    public Script script() {
        return this.script;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject(NAME);
        builder.field(ScriptField.SCRIPT.getPreferredName(), script);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new ScriptQuery(script, context.getScriptService(), context.lookup());
    }

    static class ScriptQuery extends Query {

        private final Script script;

        private final SearchScript searchScript;

        public ScriptQuery(Script script, ScriptService scriptService, SearchLookup searchLookup) {
            this.script = script;
            this.searchScript = scriptService.search(searchLookup, script, ScriptContext.Standard.SEARCH, Collections.emptyMap());
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
            return Objects.hash(super.hashCode(), script);
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

    @Override
    protected ScriptQueryBuilder doReadFrom(StreamInput in) throws IOException {
        return new ScriptQueryBuilder(Script.readScript(in));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(script);
    }

    @Override
    protected boolean doEquals(ScriptQueryBuilder other) {
        return Objects.equals(script, other.script);
    }
}
