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

package org.elasticsearch.search.suggest.filteredsuggest.filter;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.FilteredSuggestFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.InternalQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.Type;
import org.elasticsearch.search.suggest.filteredsuggest.FilteredSuggestSuggestionBuilder;
import org.elasticsearch.search.suggest.filteredsuggest.filter.FilterMappings.PathInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link FilteredSuggestFilterMapping} defines criteria that can be used to filter and/or boost suggestions at query
 * time for {@link FilteredSuggestFieldMapper}.
 *
 * Implementations have to define how filters are parsed at query/index time
 */
public abstract class FilteredSuggestFilterMapping<T extends ToXContent> implements ToXContent {

    public static final String FIELD_VALUE = "value";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_NAME = "name";

    protected final Type type;
    protected final String name;
    private boolean persist = true;

    /**
     * Define a new filter mapping of a specific type
     *
     * @param type
     *            type of filter mapping, at present {@link Type#CATEGORY}
     * @param name
     *            name of filter mapping
     */
    protected FilteredSuggestFilterMapping(Type type, String name, boolean persist) {
        this.type = type;
        this.name = name;
        this.persist = persist;
    }

    /**
     * @return the type name of the filter
     */
    protected Type type() {
        return type;
    }

    /**
     * @return the name/id of the filter
     */
    public String name() {
        return name;
    }

    protected abstract String getFieldName();

    protected abstract String getFieldIds();

    public abstract FilteredSuggestFilterValues defaultConfig();

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!persist) {
            return builder;
        }

        builder.startObject(name);
        builder.field(FIELD_TYPE, type.getName());
        toInnerXContent(builder, params);
        builder.endObject();

        return builder;
    }

    /**
     * Parses a set of index-time filters.
     */
    public abstract FilteredSuggestFilterValues parseContext(ParseContext parseContext, XContentParser parser)
            throws IOException, ElasticsearchParseException;

    /**
     * Prototype for the query filter
     */
    protected abstract T fromXContent(QueryParseContext context) throws IOException;

    /**
     * Parses query filters for this mapper
     */
    public final List<InternalQueryContext> parseQueryContext(QueryParseContext context) throws IOException, ElasticsearchParseException {
        List<T> queryContexts = new ArrayList<>();
        XContentParser parser = context.parser();
        Token token = parser.nextToken();
        if (token == Token.START_OBJECT || token == Token.VALUE_STRING) {
            queryContexts.add(fromXContent(context));
        } else if (token == Token.START_ARRAY) {
            while (parser.nextToken() != Token.END_ARRAY) {
                queryContexts.add(fromXContent(context));
            }
        }

        return toInternalQueryContexts(queryContexts);
    }

    public static Map<String, List<ContextMapping.InternalQueryContext>> parseQueries(QueryShardContext context,
            FilterMappings filterMappings, XContentParser parser) throws IOException, ElasticsearchParseException {
        if (parser == null) {
            Map<String, List<ContextMapping.InternalQueryContext>> queryFilters = new HashMap<>(1);
            List<InternalQueryContext> filters = new ArrayList<>(1);
            filters.add(new InternalQueryContext(FilteredSuggestSuggestionBuilder.EMPTY_FILTER_FILLER, 1, false));

            queryFilters.put("default_empty", filters);

            return queryFilters;
        }

        Map<String, String> filterNames = new HashMap<>(4);
        Set<PathInfo> paths = new HashSet<>();
        Map<String, List<InternalQueryContext>> querySet = new HashMap<>();

        XContentParser.Token token;

        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            String name = parser.currentName();
            final FilteredSuggestFilterMapping mapping = filterMappings.get(name);
            if (mapping == null) {
                throw new ElasticsearchParseException("no mapping defined for [" + name + "]");
            }
            filterNames.put(mapping.getFieldName(), name);
            paths.add(new PathInfo(mapping.getFieldIds(), mapping.getFieldName(), name));
            querySet.put(name, mapping.parseQueryContext(context.newParseContext(parser)));
        }

        if (filterNames.size() > 1) {
            List<PathInfo> uniquePaths = FilterMappings.generateUniqueHierarchies(paths);
            String filterSeparator = String.valueOf(FilteredSuggestFilterValueGenerator.FILTER_NAME_SEPARATOR);

            Map<String, List<ContextMapping.InternalQueryContext>> queries = new HashMap<>(2);
            for (PathInfo path : uniquePaths) {
                String[] fieldNames = path.getPath().split(filterSeparator);

                if (fieldNames.length > 1) {
                    List<ContextMapping.InternalQueryContext> contexts = new ArrayList<>();
                    StringBuilder builder = new StringBuilder();

                    generateDerivedQueryContext(filterNames, querySet, fieldNames, 0, contexts, builder, 1);
                    queries.put(path.getName(), contexts);
                } else {
                    queries.put(path.getName(), querySet.get(path.getName()));
                }
            }

            return queries;
        }

        return querySet;
    }

    private static void generateDerivedQueryContext(Map<String, String> filterNames, Map<String, List<InternalQueryContext>> querySet,
            String[] fieldNames, int currentIndex, List<ContextMapping.InternalQueryContext> contexts, StringBuilder builder, int boost) {
        if (currentIndex >= fieldNames.length) {
            contexts.add(new InternalQueryContext(builder.toString(), boost, false));
            return;
        }

        String filterName = filterNames.get(fieldNames[currentIndex]);

        List<InternalQueryContext> obj = querySet.get(filterName);

        for (InternalQueryContext queryContext : obj) {
            builder.append(queryContext.context);
            if (currentIndex < fieldNames.length) {
                generateDerivedQueryContext(filterNames, querySet, fieldNames, (currentIndex + 1), contexts, builder,
                        Math.max(boost, queryContext.boost));
            }
            builder.setLength(builder.length() - queryContext.context.length());
        }
    }

    /**
     * Convert query filters to common representation
     */
    protected abstract List<InternalQueryContext> toInternalQueryContexts(List<T> queryContexts);

    /**
     * Implementations should add specific configurations that need to be persisted
     */
    protected abstract XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        FilteredSuggestFilterMapping that = (FilteredSuggestFilterMapping) o;
        if (type != that.type)
            return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        try {
            return toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).string();
        } catch (IOException e) {
            return super.toString();
        }
    }

}
