/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "script";

    @Inject public ScriptFilterParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;

        boolean cache = false; // no need to cache it by default, changes a lot?
        // also, when caching, since its isCacheable is false, will result in loading all bit set...
        String script = null;
        String scriptLang = null;
        Map<String, Object> params = null;

        String filterName = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                }
            }
        }

        if (script == null) {
            throw new QueryParsingException(index, "script must be provided with a [script] filter");
        }
        if (params == null) {
            params = Maps.newHashMap();
        }

        Filter filter = new ScriptFilter(scriptLang, script, params, parseContext.scriptService());
        if (cache) {
            filter = parseContext.cacheFilter(filter);
        }
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }

    public static class ScriptFilter extends Filter {

        private final String script;

        private final Map<String, Object> params;

        private final SearchScript searchScript;

        private ScriptFilter(String scriptLang, String script, Map<String, Object> params, ScriptService scriptService) {
            this.script = script;
            this.params = params;

            SearchContext context = SearchContext.current();
            if (context == null) {
                throw new ElasticSearchIllegalStateException("No search context on going...");
            }

            this.searchScript = new SearchScript(context.lookup(), scriptLang, script, params, scriptService);
        }

        @Override public String toString() {
            StringBuilder buffer = new StringBuilder();
            buffer.append("ScriptFilter(");
            buffer.append(script);
            buffer.append(")");
            return buffer.toString();
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ScriptFilter that = (ScriptFilter) o;

            if (params != null ? !params.equals(that.params) : that.params != null) return false;
            if (script != null ? !script.equals(that.script) : that.script != null) return false;

            return true;
        }

        @Override public int hashCode() {
            int result = script != null ? script.hashCode() : 0;
            result = 31 * result + (params != null ? params.hashCode() : 0);
            return result;
        }

        @Override public DocIdSet getDocIdSet(final IndexReader reader) throws IOException {
            searchScript.setNextReader(reader);
            return new ScriptDocSet(reader, searchScript);
        }

        static class ScriptDocSet extends GetDocSet {

            private final SearchScript searchScript;

            public ScriptDocSet(IndexReader reader, SearchScript searchScript) {
                super(reader.maxDoc());
                this.searchScript = searchScript;
            }

            @Override public boolean isCacheable() {
                // not cacheable for several reasons:
                // 1. The script service is shared and holds the current reader executing against, and it
                //    gets changed on each getDocIdSet (which is fine for sequential reader search)
                // 2. If its really going to be cached (the _cache setting), its better to just load it into in memory bitset
                return false;
            }

            @Override public boolean get(int doc) throws IOException {
                Object val = searchScript.execute(doc);
                if (val == null) {
                    return false;
                }
                if (val instanceof Boolean) {
                    return (Boolean) val;
                }
                if (val instanceof Number) {
                    return ((Number) val).longValue() != 0;
                }
                throw new IOException("Can't handle type [" + val + "] in script filter");
            }
        }
    }
}