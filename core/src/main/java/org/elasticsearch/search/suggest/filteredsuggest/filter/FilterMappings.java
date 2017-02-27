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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.ContextQuery;
import org.apache.lucene.search.suggest.document.FilteredSuggestField;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.SuggestFieldFilterIdGenerator;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMapping.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class FilterMappings implements ToXContent {

    public static final String DERIVE_FILTER_NAME_PREFIX = "derive";

    public static final String FILTER_ID = "filterId";

    private final Map<String, FilteredSuggestFilterMapping> filterMappings;

    public FilterMappings(Map<String, FilteredSuggestFilterMapping> filterMappings) {
        this.filterMappings = filterMappings;
    }

    /**
     * @return number of context mappings held by this instance
     */
    public int size() {
        return filterMappings.size();
    }

    /**
     * Returns a context mapping by its name
     */
    public FilteredSuggestFilterMapping get(String name) {
        FilteredSuggestFilterMapping filterMapping = filterMappings.get(name);
        if (filterMapping == null) {
            throw new IllegalArgumentException("Unknown context name[" + name + "], must be one of " + filterMappings.keySet());
        }
        return filterMapping;
    }

    /**
     * Adds a filter-enabled field for all the defined mappings to <code>document</code> see
     * {@link org.apache.lucene.search.suggest.document.FilteredSuggestField}
     */
    public void addField(ParseContext.Document document, String name, String input, int weight,
            Map<String, FilteredSuggestFilterValues> filters) {
        Map<String, FilteredSuggestFilterValues> filterConfigs = new LinkedHashMap<>();
        for (FilteredSuggestFilterMapping mapping : filterMappings.values()) {
            FilteredSuggestFilterValues config = null;
            if (filters != null) {
                config = filters.get(mapping.name());
            }
            filterConfigs.put(mapping.name(), config == null ? mapping.defaultConfig() : config);
        }

        document.add(new FilteredSuggestField(name, input, weight, filterConfigs, document));
    }

    public static boolean isFilteredSuggestField(IndexableField field) {
        return field != null && field instanceof FilteredSuggestField;
    }

    /*
     * private class FilteredSuggestField extends ContextSuggestField { private final Map<String,
     * FilteredSuggestFilterValues> filters; private final ParseContext.Document document;
     * 
     * public FilteredSuggestField(String name, String value, int weight, Map<String, FilteredSuggestFilterValues>
     * filters, ParseContext.Document document) { super(name, value, weight); this.filters = filters; this.document =
     * document; }
     * 
     * @Override protected Iterable<CharSequence> contexts() { Set<CharSequence> filterValues = new HashSet<>();
     * 
     * for (FilteredSuggestFilterValues context : filters.values()) { context.generate(document, filterValues); }
     * 
     * // Empty filter value for empty filter query
     * filterValues.add(FilteredSuggestSuggestionBuilder.EMPTY_FILTER_FILLER);
     * 
     * return filterValues; } }
     */

    /**
     * Wraps a {@link CompletionQuery} with context queries
     *
     * @param query
     *            base completion query to wrap
     * @param queryFilters
     *            a map of context mapping name and collected query contexts
     * @return context-enabled queries
     */
    public List<ContextQuery> toContextQuery(CompletionQuery query, Map<String, List<ContextMapping.InternalQueryContext>> queryFilters) {
        List<ContextQuery> queries = new ArrayList<>(4);

        List<ContextMapping.InternalQueryContext> internalQueryContext = queryFilters.get("default_empty");
        if (internalQueryContext != null) {
            ContextQuery typedContextQuery = new ContextQuery(query);
            CharsRefBuilder scratch = new CharsRefBuilder();
            scratch.grow(1);

            ContextMapping.InternalQueryContext context = internalQueryContext.get(0);
            scratch.append(context.context);
            typedContextQuery.addContext(scratch.toCharsRef(), context.boost, !context.isPrefix);

            queries.add(typedContextQuery);

            return queries;
        }

        if (queryFilters.isEmpty() == false) {
            CharsRefBuilder scratch = new CharsRefBuilder();
            scratch.grow(1);
            for (String name : queryFilters.keySet()) {
                internalQueryContext = queryFilters.get(name);
                if (internalQueryContext != null) {
                    ContextQuery typedContextQuery = new ContextQuery(query);
                    for (ContextMapping.InternalQueryContext context : internalQueryContext) {
                        scratch.append(context.context);
                        typedContextQuery.addContext(scratch.toCharsRef(), context.boost, !context.isPrefix);
                        scratch.setLength(0);
                    }
                    queries.add(typedContextQuery);
                }
            }
        }

        return queries;
    }

    @SuppressWarnings("unchecked")
    public static FilterMappings loadMappings(Object configuration, SuggestFieldFilterIdGenerator filterIdGenerator)
            throws ElasticsearchParseException {
        if (configuration instanceof Map) {
            if (((Map<String, Object>) configuration).size() > 10) {
                throw new ElasticsearchParseException("Maximum of 10 filters allowed per suggest field.");
            }

            Map<String, Object> deriveConfigurations = generateDerivedContexts((Map<String, Object>) configuration, filterIdGenerator);

            Map<String, FilteredSuggestFilterMapping> mappings = new LinkedHashMap<>();
            for (Entry<String, Object> config : ((Map<String, Object>) configuration).entrySet()) {
                String name = config.getKey();
                mappings.put(name, loadMapping(name, (Map<String, Object>) config.getValue(), true,
                        (deriveConfigurations.containsKey(name) ? true : false)));
            }

            for (Entry<String, Object> config : deriveConfigurations.entrySet()) {
                String name = config.getKey();
                if (mappings.get(name) != null) {
                    continue;
                }
                mappings.put(name, loadMapping(name, (Map<String, Object>) config.getValue(), false, true));
            }

            return new FilterMappings(mappings);
        } else {
            throw new ElasticsearchParseException("no valid filter configuration");
        }
    }

    protected static FilteredSuggestFilterMapping loadMapping(String name, Map<String, Object> config, boolean persist,
            boolean indexFilterValue) throws ElasticsearchParseException {
        final Object argType = config.get(FilteredSuggestFilterMapping.FIELD_TYPE);

        if (argType == null) {
            throw new ElasticsearchParseException("missing [" + FilteredSuggestFilterMapping.FIELD_TYPE + "] in filter mapping");
        }

        final String type = argType.toString();

        if (ContextMapping.Type.CATEGORY.equals(ContextMapping.Type.fromString(type))) {
            return FilteredSuggestCategoryFilterMapping.load(name, config, persist, indexFilterValue);
        } else {
            throw new ElasticsearchParseException("unknown filter type[" + type + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> generateDerivedContexts(Map<String, Object> configurations,
            SuggestFieldFilterIdGenerator filterIdGenerator) {
        Set<PathInfo> paths = new HashSet<>(configurations.size());
        String filterPath;
        Integer filterId;
        for (Entry<String, Object> config : configurations.entrySet()) {
            Map<String, Object> value = (Map<String, Object>) config.getValue();
            filterPath = (String) value.get(FilteredSuggestCategoryFilterMapping.FILTER_FIELD_PATH);
            filterId = filterIdGenerator.getFilterId(filterPath);
            value.put(FILTER_ID, filterId);
            paths.add(new PathInfo(filterId.toString(), (filterPath == null) ? "" : filterPath, config.getKey()));
        }

        Map<String, Object> filterMappings = new LinkedHashMap<>();
        for (PathInfo path : generateUniqueHierarchies(paths)) {
            Map<String, Object> value = new LinkedHashMap<String, Object>(2);
            value.put(FilteredSuggestCategoryFilterMapping.FILTER_FIELD_PATH, path.getPath());
            value.put(FILTER_ID, path.getFilterId());
            value.put(FilteredSuggestFilterMapping.FIELD_TYPE, Type.CATEGORY.getName());

            filterMappings.put(path.getName(), value);
        }

        return filterMappings;
    }

    /**
     * iterate the map , for every key, itr the value list and push the values in to a stack and set the flag of each
     * such object in the value now find a the parent of the current value and do the same recursively until we reach
     * the highest parent for the current value. now in the next itr value , if the value list has any unvisited node ,
     * include them and proceed as usual
     * 
     * @param paths
     *            All filter path to generate all filter hierarchy
     * @return All unique paths
     */
    public static List<PathInfo> generateUniqueHierarchies(Set<PathInfo> paths) {
        List<PathInfo> hierarchies = new ArrayList<>(4);
        Map<Key, List<Value>> pathParentToChildrenMap = groupAndSort(paths);

        StringBuilder filterIdBuilder = new StringBuilder();
        StringBuilder pathBuilder = new StringBuilder();
        StringBuilder nameBuilder = new StringBuilder();

        for (Key key : pathParentToChildrenMap.keySet()) {
            List<PathInfo> stackList = new ArrayList<>(4);
            if (hasUnProcessed(pathParentToChildrenMap.get(key))) {
                populateStackList(key, stackList, pathParentToChildrenMap);
            }

            if (stackList.isEmpty()) {
                continue;
            }

            Collections.reverse(stackList);
            if (stackList.size() == 1) {
                hierarchies.add(stackList.get(0));
            } else {
                for (PathInfo entry : stackList) {
                    filterIdBuilder.append(entry.getFilterId()).append(FilteredSuggestFilterValueGenerator.FILTER_NAME_SEPARATOR);
                    pathBuilder.append(entry.getPath()).append(FilteredSuggestFilterValueGenerator.FILTER_NAME_SEPARATOR);
                    nameBuilder.append(entry.getName()).append(FilteredSuggestFilterValueGenerator.FILTER_NAME_SEPARATOR);
                }

                hierarchies.add(new PathInfo(filterIdBuilder.substring(0, filterIdBuilder.length() - 1),
                        pathBuilder.substring(0, pathBuilder.length() - 1), nameBuilder.substring(0, nameBuilder.length() - 1)));
                filterIdBuilder.setLength(0);
                pathBuilder.setLength(0);
                nameBuilder.setLength(0);
            }
        }

        return hierarchies;
    }

    /**
     * create a sorted map of parents to children ex: Products [Products.Prodtype], sorted reverse based on length of
     * parents the value list has to be sorted in reverse alphabetical order value is a object with two properties, one
     * flag to say if the field is already a part of the hierarachy hence no new processing required other property is
     * the fieldname itself
     * 
     * @param paths
     *            All filter path to generate all filter grouping
     * @return All paths
     */
    private static Map<Key, List<Value>> groupAndSort(Set<PathInfo> paths) {
        Map<Key, List<Value>> pathParentToChildrenMap = new TreeMap<>();

        Key key = null;
        for (PathInfo path : paths) {
            int indexOfDot = path.getPath().lastIndexOf(".");
            if (indexOfDot < 1) {
                key = new Key("");
            } else {
                key = new Key(path.getPath().substring(0, indexOfDot));
            }

            List<Value> children = pathParentToChildrenMap.get(key);
            if (children == null) {
                children = new ArrayList<>(4);
                pathParentToChildrenMap.put(key, children);
            }
            children.add(new Value(path, false));
        }

        for (Key keyIn : pathParentToChildrenMap.keySet()) {
            Collections.sort(pathParentToChildrenMap.get(keyIn));
        }

        return pathParentToChildrenMap;
    }

    private static boolean hasUnProcessed(List<Value> values) {
        if (values == null || values.isEmpty()) {
            return false;
        }

        for (Value value : values) {
            if (!value.visited) {
                return true;
            }
        }

        return false;
    }

    private static void populateStackList(Key key, List<PathInfo> stackList, Map<Key, List<Value>> pathParentToChildrenMap) {
        List<Value> children = pathParentToChildrenMap.get(key);

        if (children != null) {
            for (Value value : children) {
                stackList.add(value.fieldName);
                value.visited = true;
            }
        }

        int lastDotIndex = key.lastIndexOf(".");
        if (lastDotIndex < 0) {
            return;
        }

        key = key.subKey(0, lastDotIndex);

        populateStackList(key, stackList, pathParentToChildrenMap);
    }

    public static class PathInfo implements Comparable<PathInfo> {
        String filterId;
        String path;
        String name;

        public PathInfo(String filterId, String path, String name) {
            this.filterId = filterId;
            this.path = path;
            this.name = name;
        }

        public String getFilterId() {
            return filterId;
        }

        public String getName() {
            return name;
        }

        public String getPath() {
            return path;
        }

        @Override
        public int compareTo(PathInfo o) {
            return this.path.compareTo(o.path);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PathInfo other = (PathInfo) obj;
            if (path == null) {
                if (other.path != null)
                    return false;
            } else if (!path.equals(other.path))
                return false;
            return true;
        }

    }

    private static class Value implements Comparable<Value> {
        PathInfo fieldName;
        boolean visited;

        Value(PathInfo fieldName, boolean b) {
            this.fieldName = fieldName;
            visited = b;
        }

        @Override
        /**
         * reverse order of string comparator
         */
        public int compareTo(Value o) {
            if (fieldName == null || o.fieldName == null) {
                return 0;
            }

            return o.fieldName.compareTo(fieldName);
        }

        @Override
        public String toString() {
            return fieldName.toString();
        }
    }

    private static class Key implements Comparable<Key> {
        String name;
        Integer length;

        Key(String name) {
            this.name = name;
            this.length = this.name.length();
        }

        public Key subKey(int beginIndex, int endIndex) {
            return new Key(name.substring(beginIndex, endIndex));
        }

        public int lastIndexOf(String str) {
            return name.lastIndexOf(str);
        }

        @Override
        /**
         * order of based on length of name
         */
        public int compareTo(Key o) {
            if (this.length < o.length) {
                return 1;
            }

            if (this.length > o.length) {
                return -1;
            }

            int compareTo = this.name.compareTo(o.name);
            if (compareTo == 0) {
                return compareTo;
            }

            if (compareTo < 0) {
                return 1;
            }

            return -1;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Writes a list of objects specified by the defined {@link FilterMappings}
     *
     * see {@link FilteredSuggestFilterMapping#toXContent(XContentBuilder, Params)}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (FilteredSuggestFilterMapping filterMapping : filterMappings.values()) {
            filterMapping.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterMappings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj instanceof FilterMappings) == false) {
            return false;
        }
        FilterMappings other = ((FilterMappings) obj);
        return filterMappings.equals(other.filterMappings);
    }
}
