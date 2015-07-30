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

import org.apache.lucene.search.suggest.xdocument.CompletionQuery;
import org.apache.lucene.search.suggest.xdocument.ContextQuery;
import org.apache.lucene.search.suggest.xdocument.ContextSuggestField;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocs;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionContext;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.search.suggest.completion.context.ContextMapping.*;

/**
 * ContextMappings defines criteria for filtering and/or boosting
 * suggestions at query time. A ContextMappings manages a set of {@link ContextMapping}s
 * for a {@link CompletionFieldMapper}.
 * A {@link ContextMapping} defines the criterion type, how it is parsed and queried.
 *
 * ContextMappings delegates context parsing to appropriate {@link ContextMapping}s,
 * creates context queries and indexes context-enabled suggestion fields
 * for the defined {@link ContextMapping}s
 */
public class ContextMappings implements ToXContent {
    private final LinkedHashMap<String, ContextMapping<?>> contextMappings;
    private final List<ContextMapping<?>> queryOrder;

    private ContextMappings(LinkedHashMap<String, ContextMapping<?>> contextMappings) {
        this.contextMappings = contextMappings;
        this.queryOrder = toQueryOrder(contextMappings.values());
    }

    private static List<ContextMapping<?>> toQueryOrder(Collection<ContextMapping<?>> contextMappings) {
        List<ContextMapping<?>> queryOrder = new ArrayList<>(contextMappings.size());
        for (ContextMapping<?> mapping : contextMappings) {
            queryOrder.add(0, mapping);
        }
        return queryOrder;
    }

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
    public Map<String, QueryContexts> parseQueryContext(XContentParser parser) throws IOException {
        Map<String, QueryContexts> queryContextsMap = new HashMap<>(contextMappings.size());
        assert parser.currentToken() == Token.START_OBJECT;
        Token token;
        String currentFieldName;
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                if (contextMappings.containsKey(currentFieldName) == false) {
                    throw new IllegalArgumentException("Unknown context name[" + currentFieldName + "], must be one of " + contextMappings.keySet());
                } else {
                    final ContextMapping<?> mapping = contextMappings.get(currentFieldName);
                    queryContextsMap.put(currentFieldName, mapping.parseQueryContext(currentFieldName, parser));
                }
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
    public Map<String, Set<CharSequence>> parseContext(ParseContext parseContext, XContentParser parser) throws IOException {
        Map<String, Set<CharSequence>> contextMap = new HashMap<>(contextMappings.size());
        Token token = parser.currentToken();
        if (token == Token.START_ARRAY) {
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                while ((token = parser.nextToken()) != Token.END_OBJECT) {
                    if (token == Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        ContextMapping<?> contextMapping = contextMappings.get(currentFieldName);
                        if (contextMapping == null) {
                            throw new IllegalArgumentException("context [" + currentFieldName + "] is not defined");
                        }
                        Set<CharSequence> contexts = contextMap.get(currentFieldName);
                        if (contexts == null) {
                            contexts = new HashSet<>();
                        }
                        contexts.addAll(contextMapping.parseContext(parseContext, parser));
                        contextMap.put(currentFieldName, contexts);
                    }
                }
            }
        } else if (token == Token.START_OBJECT) {
            ContextMapping<?> contextMapping = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    contextMapping = contextMappings.get(currentFieldName);
                    if (contextMapping == null) {
                        throw new IllegalArgumentException("context [" + currentFieldName + "] is not defined");
                    }
                } else if (token == Token.VALUE_STRING || token == Token.START_ARRAY || token == Token.START_OBJECT) {
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

    /**
     * Adds a context-enabled field for all the defined mappings to <code>document</code>
     * see {@link ContextField}
     */
    public void addFields(ParseContext.Document document, String name, String input, int weight, Map<String, Set<CharSequence>> contexts) {
        document.add(new ContextField(name, input, weight, contexts, document));
    }

    /**
     * Wraps a {@link CompletionQuery} with context queries,
     * delegates to {@link ContextMapping#toContextQuery(CompletionQuery, QueryContexts)}s
     * with the right {@link QueryContexts}.
     * see {@link ContextField} for nested context query ordering
     *
     * @param query base completion query to wrap
     * @param queryContexts a map of context mapping name and collected query contexts
     *                      as provided by {@link #parseQueryContext(XContentParser)}
     * @return a context-enabled query
     */
    public CompletionQuery toContextQuery(CompletionQuery query, Map<String, QueryContexts> queryContexts) {
        CompletionQuery completionQuery = query;
        for (ContextMapping<?> contextMapping : queryOrder) {
            completionQuery = contextMapping.toContextQuery(completionQuery, queryContexts.get(contextMapping.name()));
        }
        return completionQuery;
    }

    /**
     * Maps an output context list to a map of context mapping names and their values
     *
     * see {@link ContextField}
     * @param contexts from {@link TopSuggestDocs.SuggestScoreDoc#contexts}
     * @return a map of context names and their values
     *
     */
    public Map<String, CharSequence> buildContextValueMap(CharSequence[] contexts) {
        assert contexts.length == contextMappings.size();
        Map<String, CharSequence> contextMap = new LinkedHashMap<>(contextMappings.size());
        int upto = 0;
        for (String name : contextMappings.keySet()) {
            contextMap.put(name, contexts[upto++]);
        }
        return contextMap;
    }

    /**
     * Loads {@link ContextMappings} from configuration
     *
     * Expected configuration:
     *  List of maps representing {@link ContextMapping}
     *  [{"name": .., "type": .., ..}, {..}]
     *
     */
    public static ContextMappings load(Object configuration, Version indexVersionCreated) throws ElasticsearchParseException {
        LinkedHashMap<String, ContextMapping<?>> contextMappings = new LinkedHashMap<>();
        if (configuration instanceof List) {
            List<Object> configurations = (List<Object>)configuration;
            for (Object conf : configurations) {
                Map<String, Object> contextConf = ((Map<String, Object>) conf);
                String name = (String) contextConf.get("name");
                contextConf.remove("name");
                contextMappings.put(name, loadMapping(name, contextConf, indexVersionCreated));
            }
            if (contextMappings.size() == 0) {
                throw new ElasticsearchParseException("expected a single context mapping");
            }
        } else {
            throw new ElasticsearchParseException("expected a list of context mapping");
        }
        return new ContextMappings(contextMappings);
    }

    private static ContextMapping<?> loadMapping(String name, Map<String, Object> config, Version indexVersionCreated)
            throws ElasticsearchParseException {
        final Object argType = config.get(FIELD_TYPE);

        if (argType == null) {
            throw new ElasticsearchParseException("missing [" + FIELD_TYPE + "] in context mapping");
        }

        final String type = argType.toString();
        ContextMapping contextMapping;
        Type contextMappingType = Type.fromString(type);
        switch (contextMappingType) {
            case CATEGORY:
                contextMapping = CategoryContextMapping.load(name, config);
                break;
            case GEO:
                contextMapping = GeoContextMapping.load(name, config);
                break;
            default:
                throw new ElasticsearchParseException("unknown context type[" + type + "]");

        }
        config.remove(FIELD_TYPE);
        DocumentMapperParser.checkNoRemainingFields(name, config, indexVersionCreated);

        return contextMapping;
    }

    private LinkedHashMap<String, Set<CharSequence>> parseContext(ParseContext.Document document) {
        LinkedHashMap<String, Set<CharSequence>> results = new LinkedHashMap<>(contextMappings.size());
        for (ContextMapping<?> contextMapping : contextMappings.values()) {
            results.put(contextMapping.name(), contextMapping.parseContext(document));
        }
        return results;
    }

    /**
     * Writes a list of objects specified by the defined {@link ContextMapping}s
     *
     * see {@link ContextMapping#toXContent(XContentBuilder, Params)}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (ContextMapping<?> contextMapping : contextMappings.values()) {
            builder.startObject();
            contextMapping.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        for (Map.Entry<String, ContextMapping<?>> mappingEntry : contextMappings.entrySet()) {
            result = prime * result + mappingEntry.getKey().hashCode();
            result = prime * result + mappingEntry.getValue().hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj instanceof ContextMappings) == false) {
            return false;
        }
        ContextMappings other = ((ContextMappings) obj);
        return contextMappings.equals(other.contextMappings);
    }

    /**
     * An extension to {@link ContextSuggestField} which can index
     * suggestion values with multiple contexts.
     * <p>
     * ContextSuggestField uses {@link ContextSuggestField#CONTEXT_SEPARATOR}
     * as a marker between a context and suggestion value:
     * <p>
     * CONTEXT_VALUE | CONTEXT_SEPARATOR | SUGGESTION_VALUE
     * <p>
     * Multiple contexts are indexed in the format of:
     * <p>
     * CONTEXT_VALUE = VALUE_CONTEXT1 | CONTEXT_SEPARATOR | VALUE_CONTEXT2 | ...
     * <p>
     * in order to map context values back to the context mapping,
     * the context value has to be indexed in a specific order.
     * This ordering is defined by {@link ContextMappings#contextMappings}.
     * <p>
     * To construct appropriate nested {@link ContextQuery} for multiple contexts,
     * the reverse ordering is used. see {@link ContextMappings#queryOrder}.
     * <p>
     * To map back context values from a suggestion hit, the same
     * ordering is used. see {@link #buildContextValueMap(CharSequence[])}
     *
     * TODO: another approach would be associating a type with
     * contexts, that way we could index a context-enabled
     * suggestion as
     * <p>
     *     CONTEXT_VALUE | CONTEXT_SEPARATOR | SUGGESTION_VALUE
     * <p>
     * keeping one context per suggestion
     *
     */
    private class ContextField extends ContextSuggestField {

        private final ParseContext.Document document;
        private final Map<String, Set<CharSequence>> contextsMap;

        private final CharsRefBuilder scratch = new CharsRefBuilder();

        private ContextField(String name, String value, int weight, Map<String, Set<CharSequence>> contextsMap, ParseContext.Document document) {
            super(name, value, weight);
            this.contextsMap = contextsMap;
            this.document = document;
        }

        @Override
        protected Set<CharSequence> contexts() {
            LinkedHashMap<String, Set<CharSequence>> currentContextsMap = parseContext(document);
            // merge with indexed contexts if any
            for (Map.Entry<String, Set<CharSequence>> context : contextsMap.entrySet()) {
                Set<CharSequence> contexts = currentContextsMap.get(context.getKey());
                if (contexts == null) {
                    contexts = context.getValue();
                } else {
                    contexts.addAll(context.getValue());
                }
                currentContextsMap.put(context.getKey(), contexts);
            }
            List<Set<CharSequence>> contextsList = new ArrayList<>(currentContextsMap.size());
            for (Set<CharSequence> contexts : currentContextsMap.values()) {
                contextsList.add(contexts);
            }
            Set<CharSequence> initialContexts = contextsList.remove(0);
            return toContexts(contextsList.iterator(), initialContexts);
        }

        private Set<CharSequence> toContexts(Iterator<Set<CharSequence>> orderedContextsIterator, Set<CharSequence> contexts) {
            if (orderedContextsIterator.hasNext()) {
                Set<CharSequence> currentContexts = orderedContextsIterator.next();
                Set<CharSequence> contextResults = new HashSet<>();
                Iterator<CharSequence> contextIterator = contexts.iterator();
                if (contextIterator.hasNext()) {
                    while (contextIterator.hasNext()) {
                        scratch.append(contextIterator.next());
                        addCurrentContextValues(currentContexts, contextResults, scratch);
                        contextIterator.remove();
                    }
                } else {
                    addCurrentContextValues(currentContexts, contextResults, scratch);
                }
                contexts.addAll(contextResults);
                return orderedContextsIterator.hasNext() ? toContexts(orderedContextsIterator, contexts) : contexts;
            } else {
                return contexts;
            }
        }

        private void addCurrentContextValues(Set<CharSequence> currentContexts, Set<CharSequence> mergedContexts, CharsRefBuilder scratch) {
            scratch.append((char) CONTEXT_SEPARATOR);
            int offset = scratch.length();
            // handle null case
            if (currentContexts.size() == 0) {
                mergedContexts.add(scratch.toCharsRef());
            } else {
                for (CharSequence currentContext : currentContexts) {
                    int newLength = offset + currentContext.length();
                    scratch.setLength(offset);
                    scratch.append(currentContext);
                    scratch.setLength(newLength);
                    mergedContexts.add(scratch.toCharsRef());
                }
            }
            scratch.clear();
        }
    }
}
