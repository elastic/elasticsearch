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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;

import java.io.IOException;
import java.util.*;

/**
 * A {@link ContextMapping} defines criteria types that can be used to
 * filter and/or boost suggestions at query time for {@link CompletionFieldMapper}.
 * Implementations have to define how contexts are parsed at query/index time,
 * a representation of query contexts to be used and how to construct
 * an appropriate query for the type
 *
 * @param <T> query context type
 */
public abstract class ContextMapping<T extends ToXContent> implements ToXContent {

    public static final String FIELD_TYPE = "type";
    public static final String FIELD_NAME = "name";
    protected final Type type;
    protected final String name;

    public enum Type {
        CATEGORY, GEO;

        public static Type fromString(String type) {
            if (type.equalsIgnoreCase("category")) {
                return CATEGORY;
            } else if (type.equalsIgnoreCase("geo")) {
                return GEO;
            } else {
                throw new IllegalArgumentException("No context type for [" + type + "]");
            }
        }
    }

    /**
     * Define a new context mapping of a specific type
     *
     * @param type type of context mapping, either {@link Type#CATEGORY} or {@link Type#GEO}
     * @param name name of context mapping
     */
    protected ContextMapping(Type type, String name) {
        super();
        this.type = type;
        this.name = name;
    }

    /**
     * @return the type name of the context
     */
    public Type type() {
        return type;
    }

    /**
     * @return the name/id of the context
     */
    public String name() {
        return name;
    }

    /**
     * Parses a set of index-time contexts.
     */
    protected abstract Set<CharSequence> parseContext(ParseContext parseContext, XContentParser parser) throws IOException, ElasticsearchParseException;

    /**
     * Retrieves a set of context from a <code>document</code> at index-time.
     */
    protected abstract Set<CharSequence> parseContext(ParseContext.Document document);

    /**
     * Parses query contexts for this mapper
     */
    protected abstract QueryContexts<T> parseQueryContext(String name, XContentParser parser) throws IOException, ElasticsearchParseException;

    /**
     * Named holder for a set of query context
     * @param <T> query context type
     */
    public static class QueryContexts<T extends ToXContent> implements Iterable<T>, ToXContent {
        private String name;
        private List<T> queryContexts;

        /**
         * Constructs a query contexts holder
         * for a context mapping
         * @param name name of the context mapping to
         *             query against
         */
        public QueryContexts(String name) {
            this.name = name;
            this.queryContexts = new ArrayList<>();
        }

        /**
         * @return name of the context mapping
         * to query against
         */
        public String getName() {
            return name;
        }

        /**
         * Adds a query context to the holder
         * @param queryContext instance
         */
        public void add(T queryContext) {
            this.queryContexts.add(queryContext);
        }

        @Override
        public Iterator<T> iterator() {
            return queryContexts.iterator();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(name);
            for (T queryContext : queryContexts) {
                queryContext.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }
    }

    /**
     * Wrap a completion query using the query context
     *
     * @param query base completion query
     * @param queryContexts represents query contexts for this context mapping
     * @return a context-enabled completion query
     */
    protected abstract CompletionQuery toContextQuery(CompletionQuery query, @Nullable QueryContexts<T> queryContexts);

    /**
     * Implementations should add specific configurations
     * that need to be persisted
     */
    protected abstract XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_NAME, name);
        builder.field(FIELD_TYPE, type.name());
        toInnerXContent(builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + type.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj instanceof ContextMapping) == false) {
            return false;
        }
        ContextMapping other = ((ContextMapping) obj);
        return name.equals(other.name) && type == other.type;
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
