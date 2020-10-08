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

import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.ContextQuery;
import org.apache.lucene.search.suggest.document.ContextSuggestField;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.search.suggest.completion.context.ContextMapping.FIELD_NAME;
import static org.elasticsearch.search.suggest.completion.context.ContextMapping.FIELD_TYPE;

/**
 * ContextMappings indexes context-enabled suggestion fields
 * and creates context queries for defined {@link ContextMapping}s
 * for a {@link CompletionFieldMapper}
 */
public class ContextMappings implements ToXContent, Iterable<ContextMapping<?>> {

    private final List<ContextMapping<?>> contextMappings;
    private final Map<String, ContextMapping<?>> contextNameMap;

    public ContextMappings(List<ContextMapping<?>> contextMappings) {
        if (contextMappings.size() > 255) {
            // we can support more, but max of 255 (1 byte) unique context types per suggest field
            // seems reasonable?
            throw new UnsupportedOperationException("Maximum of 10 context types are supported was: " + contextMappings.size());
        }
        this.contextMappings = contextMappings;
        contextNameMap = new HashMap<>(contextMappings.size());
        for (ContextMapping<?> mapping : contextMappings) {
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
        ContextMapping<?> contextMapping = contextNameMap.get(name);
        if (contextMapping == null) {
            List<String> keys = new ArrayList<>(contextNameMap.keySet());
            Collections.sort(keys);
            throw new IllegalArgumentException("Unknown context name [" + name + "], must be one of " + keys.toString());
        }
        return contextMapping;
    }

    /**
     * Adds a context-enabled field for all the defined mappings to <code>document</code>
     * see {@link org.elasticsearch.search.suggest.completion.context.ContextMappings.TypedContextField}
     */
    public void addField(ParseContext.Document document, String name, String input, int weight, Map<String, Set<String>> contexts) {
        document.add(new TypedContextField(name, input, weight, contexts, document));
    }

    @Override
    public Iterator<ContextMapping<?>> iterator() {
        return contextMappings.iterator();
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
    private class TypedContextField extends ContextSuggestField {
        private final Map<String, Set<String>> contexts;
        private final ParseContext.Document document;

        TypedContextField(String name, String value, int weight, Map<String, Set<String>> contexts,
                          ParseContext.Document document) {
            super(name, value, weight);
            this.contexts = contexts;
            this.document = document;
        }

        @Override
        protected Iterable<CharSequence> contexts() {
            Set<CharsRef> typedContexts = new HashSet<>();
            final CharsRefBuilder scratch = new CharsRefBuilder();
            scratch.grow(1);
            for (int typeId = 0; typeId < contextMappings.size(); typeId++) {
                scratch.setCharAt(0, (char) typeId);
                scratch.setLength(1);
                ContextMapping<?> mapping = contextMappings.get(typeId);
                Set<String> contexts = new HashSet<>(mapping.parseContext(document));
                if (this.contexts.get(mapping.name()) != null) {
                    contexts.addAll(this.contexts.get(mapping.name()));
                }
                for (String context : contexts) {
                    scratch.append(context);
                    typedContexts.add(scratch.toCharsRef());
                    scratch.setLength(1);
                }
            }
            if (typedContexts.isEmpty()) {
                throw new IllegalArgumentException("Contexts are mandatory in context enabled completion field [" + name + "]");
            }
            return new ArrayList<CharSequence>(typedContexts);
        }
    }

    /**
     * Wraps a {@link CompletionQuery} with context queries
     *
     * @param query base completion query to wrap
     * @param queryContexts a map of context mapping name and collected query contexts
     * @return a context-enabled query
     */
    public ContextQuery toContextQuery(CompletionQuery query, Map<String, List<ContextMapping.InternalQueryContext>> queryContexts) {
        ContextQuery typedContextQuery = new ContextQuery(query);
        boolean hasContext = false;
        if (queryContexts.isEmpty() == false) {
            CharsRefBuilder scratch = new CharsRefBuilder();
            scratch.grow(1);
            for (int typeId = 0; typeId < contextMappings.size(); typeId++) {
                scratch.setCharAt(0, (char) typeId);
                scratch.setLength(1);
                ContextMapping<?> mapping = contextMappings.get(typeId);
                List<ContextMapping.InternalQueryContext> internalQueryContext = queryContexts.get(mapping.name());
                if (internalQueryContext != null) {
                    for (ContextMapping.InternalQueryContext context : internalQueryContext) {
                        scratch.append(context.context);
                        typedContextQuery.addContext(scratch.toCharsRef(), context.boost, !context.isPrefix);
                        scratch.setLength(1);
                        hasContext = true;
                    }
                }
            }
        }
        if (hasContext == false) {
            throw new IllegalArgumentException("Missing mandatory contexts in context query");
        }
        return typedContextQuery;
    }

    /**
     * Maps an output context list to a map of context mapping names and their values
     *
     * see {@link org.elasticsearch.search.suggest.completion.context.ContextMappings.TypedContextField}
     * @return a map of context names and their values
     *
     */
    public Map<String, Set<String>> getNamedContexts(List<CharSequence> contexts) {
        Map<String, Set<String>> contextMap = new HashMap<>(contexts.size());
        for (CharSequence typedContext : contexts) {
            int typeId = typedContext.charAt(0);
            assert typeId < contextMappings.size() : "Returned context has invalid type";
            ContextMapping<?> mapping = contextMappings.get(typeId);
            Set<String> contextEntries = contextMap.get(mapping.name());
            if (contextEntries == null) {
                contextEntries = new HashSet<>();
                contextMap.put(mapping.name(), contextEntries);
            }
            contextEntries.add(typedContext.subSequence(1, typedContext.length()).toString());
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
        final List<ContextMapping<?>> contextMappings;
        if (configuration instanceof List) {
            contextMappings = new ArrayList<>();
            List<Object> configurations = (List<Object>) configuration;
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

    private static ContextMapping<?> load(Map<String, Object> contextConfig, Version indexVersionCreated) {
        String name = extractRequiredValue(contextConfig, FIELD_NAME);
        String type = extractRequiredValue(contextConfig, FIELD_TYPE);
        final ContextMapping<?> contextMapping;
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
        return Objects.hash(contextMappings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj instanceof ContextMappings) == false) {
            return false;
        }
        ContextMappings other = ((ContextMappings) obj);
        return contextMappings.equals(other.contextMappings);
    }

    @Override
    public String toString() {
        return contextMappings.stream().map(ContextMapping::toString).collect(Collectors.joining(",", "[", "]"));
    }
}
