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
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
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
 * ContextMappings indexes context-enabled suggestion fields
 * and creates context queries for defined {@link ContextMapping}s
 * for a {@link CompletionFieldMapper}
 */
public class ContextMappings implements ToXContent {
    private final List<ContextMapping> contextMappings;
    private final Map<String, ContextMapping> contextNameMap;

    private ContextMappings(List<ContextMapping> contextMappings) {
        if (contextMappings.size() > 255) {
            // we can support more, but max of 255 (1 byte) unique context types per suggest field
            // seems reasonable?
            throw new UnsupportedOperationException("Maximum of 10 context types are supported was: " + contextMappings.size());
        }
        this.contextMappings = contextMappings;
        contextNameMap = new HashMap<>(contextMappings.size());
        for (ContextMapping mapping : contextMappings) {
            contextNameMap.put(mapping.name(), mapping);
        }
    }

    /**
     * @return number of context mappings
     * held by this instance
     */
    public int size() {
        return contextMappings.size();
    }

    /**
     * Returns a context mapping by its name
     */
    public ContextMapping<?> get(String name) {
        ContextMapping contextMapping = contextNameMap.get(name);
        if (contextMapping == null) {
            throw new IllegalArgumentException("Unknown context name[" + name + "], must be one of " + contextNameMap.size());
        }
        return contextMapping;
    }

    /**
     * Adds a context-enabled field for all the defined mappings to <code>document</code>
     * see {@link org.elasticsearch.search.suggest.completion.context.ContextMappings.TypedContextField}
     */
    public void addFields(ParseContext.Document document, String name, String input, int weight, Map<String, Set<CharSequence>> contexts) {
        for (int typeId = 0; typeId < contextMappings.size(); typeId++) {
            ContextMapping<?> mapping = contextMappings.get(typeId);
            Set<CharSequence> ctxs = contexts.get(mapping.name());
            if (ctxs == null) {
                ctxs = new HashSet<>();
            }
            document.add(new TypedContextField(name, typeId, input, weight, ctxs, document, mapping));
        }
    }

    /**
     * Field prepends context values with a suggestion
     * Context values are associated with a type, denoted by
     * a type id, which is prepended to the context value.
     *
     * Every defined context mapping yields a unique type id (index of the
     * corresponding context mapping in the context mappings list)
     * for all its context values
     *
     * The type, context and suggestion values are encoded as follows:
     * <p>
     *     TYPE_ID | CONTEXT_VALUE | CONTEXT_SEP | SUGGESTION_VALUE
     * </p>
     *
     * Field can also use values of other indexed fields as contexts
     * at index time
     */
    private static class TypedContextField extends ContextSuggestField {
        private final int typeId;
        private final Set<CharSequence> contexts;
        private final ParseContext.Document document;
        private final ContextMapping<?> mapping;

        public TypedContextField(String name, int typeId, String value, int weight, Set<CharSequence> contexts,
                                 ParseContext.Document document, ContextMapping mapping) {
            super(name, value, weight);
            this.typeId = typeId;
            this.contexts = contexts;
            this.document = document;
            this.mapping = mapping;
        }

        @Override
        protected Iterable<CharSequence> contexts() {
            contexts.addAll(mapping.parseContext(document));
            final Iterator<CharSequence> contextsIterator = contexts.iterator();
            final CharsRefBuilder scratch = new CharsRefBuilder();
            scratch.append(((char) typeId));
            return new Iterable<CharSequence>() {
                @Override
                public Iterator<CharSequence> iterator() {
                    return new Iterator<CharSequence>() {
                        @Override
                        public boolean hasNext() {
                            return contextsIterator.hasNext();
                        }

                        @Override
                        public CharSequence next() {
                            scratch.setLength(1);
                            scratch.append(contextsIterator.next());
                            return scratch.toCharsRef();
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException("remove not supported");
                        }
                    };
                }
            };
        }
    }

    /**
     * Wraps a {@link CompletionQuery} with context queries,
     * individual context mappings adds query contexts using
     * {@link ContextMapping#addQueryContexts(ContextQuery, QueryContexts)}s
     *
     * @param query base completion query to wrap
     * @param queryContexts a map of context mapping name and collected query contexts
     *                      see {@link ContextMappingsParser#parseQueryContext(ContextMappings, XContentParser)}
     * @return a context-enabled query
     */
    public ContextQuery toContextQuery(CompletionQuery query, Map<String, QueryContexts> queryContexts) {
        CharsRefBuilder scratch = new CharsRefBuilder();
        TypedContextQuery contextQuery = new TypedContextQuery(query, scratch);
        for (int typeId = 0; typeId < contextMappings.size(); typeId++) {
            ContextMapping<?> mapping = contextMappings.get(typeId);
            contextQuery.setTypeId(typeId);
            QueryContexts queryContext = queryContexts.get(mapping.name());
            if (queryContext != null) {
                if (queryContext.size() == 0) {
                    contextQuery.addAllContexts();
                } else {
                    mapping.addQueryContexts(contextQuery, queryContext);
                }
            }
        }
        return contextQuery;
    }

    /**
     * Wraps a Context query to prepend the context values
     * with a type id
     */
    private static class TypedContextQuery extends ContextQuery {
        private final CharsRefBuilder scratch;

        public TypedContextQuery(CompletionQuery innerQuery, CharsRefBuilder scratch) {
            super(innerQuery);
            this.scratch = scratch;
        }

        public void setTypeId(int typeId) {
            scratch.clear();
            scratch.append(((char) typeId));
        }

        @Override
        public final void addContext(CharSequence context, float boost, boolean exact) {
            scratch.setLength(1);
            if (context != null) {
                scratch.append(context);
            }
            super.addContext(scratch.toCharsRef(), boost, exact);
        }

        @Override
        public final void addAllContexts() {
            addContext(null, 1, false);
        }
    }

    /**
     * Maps an output context list to a map of context mapping names and their values
     *
     * see {@link org.elasticsearch.search.suggest.completion.context.ContextMappings.TypedContextField}
     * @param context from {@link TopSuggestDocs.SuggestScoreDoc#context}
     * @return a map of context names and their values
     *
     */
    public Map.Entry<String, CharSequence> getNamedContext(CharSequence context) {
        int typeId = context.charAt(0);
        assert typeId < contextMappings.size() : "Returned context has invalid type";
        ContextMapping<?> mapping = contextMappings.get(typeId);
        return new AbstractMap.SimpleEntry<>(mapping.name(), context.subSequence(1, context.length()));
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
        final List<ContextMapping> contextMappings;
        if (configuration instanceof List) {
            contextMappings = new ArrayList<>();
            List<Object> configurations = (List<Object>)configuration;
            for (Object contextConfig : configurations) {
                contextMappings.add(load((Map<String, Object>) contextConfig, indexVersionCreated));
            }
            if (contextMappings.size() == 0) {
                throw new ElasticsearchParseException("expected at least one context mapping");
            }
        } else if (configuration instanceof Map) {
            contextMappings = Collections.singletonList(load(((Map<String, Object>) configuration), indexVersionCreated));
        } else {
            throw new ElasticsearchParseException("expected a list or an entry of context mapping");
        }
        return new ContextMappings(contextMappings);
    }

    private static ContextMapping load(Map<String, Object> contextConfig, Version indexVersionCreated) {
        String name = extractRequiredValue(contextConfig, FIELD_NAME);
        String type = extractRequiredValue(contextConfig, FIELD_TYPE);
        final ContextMapping contextMapping;
        switch (Type.fromString(type)) {
            case CATEGORY:
                contextMapping = CategoryContextMapping.load(name, contextConfig);
                break;
            case GEO:
                contextMapping = GeoContextMapping.load(name, contextConfig);
                break;
            default:
                throw new ElasticsearchParseException("unknown context type[" + type + "]");
        }
        DocumentMapperParser.checkNoRemainingFields(name, contextConfig, indexVersionCreated);
        return contextMapping;
    }

    private static String extractRequiredValue(Map<String, Object> contextConfig, String paramName) {
        final Object paramValue = contextConfig.get(paramName);
        if (paramValue == null) {
            throw new ElasticsearchParseException("missing [" + paramName + "] in context mapping");
        }
        contextConfig.remove(paramName);
        return paramValue.toString();
    }

    /**
     * Writes a list of objects specified by the defined {@link ContextMapping}s
     *
     * see {@link ContextMapping#toXContent(XContentBuilder, Params)}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (ContextMapping<?> contextMapping : contextMappings) {
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
        for (ContextMapping<?> contextMapping : contextMappings) {
            result = prime * result + contextMapping.hashCode();
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

}
