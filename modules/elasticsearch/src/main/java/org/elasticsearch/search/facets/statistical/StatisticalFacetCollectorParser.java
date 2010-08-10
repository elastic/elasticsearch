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

package org.elasticsearch.search.facets.statistical;

import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.collector.FacetCollector;
import org.elasticsearch.search.facets.collector.FacetCollectorParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class StatisticalFacetCollectorParser implements FacetCollectorParser {

    private static ThreadLocal<ThreadLocals.CleanableValue<Map<String, Object>>> cachedParams = new ThreadLocal<ThreadLocals.CleanableValue<Map<String, Object>>>() {
        @Override protected ThreadLocals.CleanableValue<Map<String, Object>> initialValue() {
            return new ThreadLocals.CleanableValue<Map<String, Object>>(new HashMap<String, Object>());
        }
    };

    public static final String NAME = "statistical";

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String field = null;

        String currentFieldName = null;
        String script = null;
        Map<String, Object> params = cachedParams.get().get();
        params.clear();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                }
            }
        }
        if (script == null && field == null) {
            throw new FacetPhaseExecutionException(facetName, "statistical facet requires either [script] or [field] to be set");
        }
        if (field != null) {
            return new StatisticalFacetCollector(facetName, field, context);
        } else {
            return new ScriptStatisticalFacetCollector(facetName, script, params, context);
        }
    }
}