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

package org.elasticsearch.search.suggest.completion.context;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Parses index-time context values and query contexts
 * for all defined context mappings in
 * {@link ContextMappings}
 */
public class ContextMappingsParser {

    /**
     * Parses query contexts for all the defined contexts
     *
     * Used in {@link CompletionSuggester#toQuery(CompletionSuggestionContext)}
     * <br>
     * Expected Input:
     * <ul>
     *     <li><pre>{&quot;NAME&quot;: <i>&lt;QUERY_CONTEXTS&gt;</i>, ..}</pre</li>
     * </ul>
     * see specific {@link ContextMapping#parseQueryContext(String, XContentParser)} implementation
     * for QUERY_CONTEXTS
     * NAME refers to the name of context mapping
     */
    public static Map<String, ContextMapping.QueryContexts> parseQueryContext(ContextMappings contextMappings, XContentParser parser) throws IOException {
        Map<String, ContextMapping.QueryContexts> queryContextsMap = new HashMap<>(contextMappings.size());
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        XContentParser.Token token;
        String currentFieldName;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                final ContextMapping<?> mapping = contextMappings.get(currentFieldName);
                queryContextsMap.put(currentFieldName, mapping.parseQueryContext(currentFieldName, parser));
            }

        }
        return queryContextsMap;
    }

    /**
     * Parses index-time contexts for all the defined context mappings
     *
     * Expected input :
     *  <ul>
     *     <li>Array: <pre>[{<i>&lt;NAME&gt;</i>: <i>&lt;CONTEXTS&gt;</i>}, ]</pre></li>
     *     <li>Object: <pre>{<i>&lt;NAME&gt;</i>: <i>&lt;CONTEXTS&gt;</i>}</pre></li>
     *  </ul>
     *
     *  see specific {@link ContextMapping#parseContext(ParseContext, XContentParser)} implementations
     *  for CONTEXTS.
     *  NAME refers to the name of context mapping
     */
    public static Map<String, Set<CharSequence>> parseContext(ContextMappings contextMappings, ParseContext parseContext, XContentParser parser) throws IOException {
        Map<String, Set<CharSequence>> contextMap = new HashMap<>(contextMappings.size());
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        ContextMapping<?> mapping = contextMappings.get(currentFieldName);
                        Set<CharSequence> contexts = contextMap.get(currentFieldName);
                        if (contexts == null) {
                            contexts = new HashSet<>();
                        }
                        contexts.addAll(mapping.parseContext(parseContext, parser));
                        contextMap.put(currentFieldName, contexts);
                    }
                }
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            ContextMapping<?> contextMapping = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    contextMapping = contextMappings.get(currentFieldName);
                } else if (token == XContentParser.Token.VALUE_STRING || token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT) {
                    assert currentFieldName != null;
                    Set<CharSequence> contexts = contextMap.get(currentFieldName);
                    if (contexts == null) {
                        contexts = new HashSet<>();
                    }
                    contexts.addAll(contextMapping.parseContext(parseContext, parser));
                    contextMap.put(currentFieldName, contexts);
                }
            }
        }
        return contextMap;
    }
}
