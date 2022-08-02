/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion.context;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link ContextMapping} defines criteria that can be used to
 * filter and/or boost suggestions at query time for {@link CompletionFieldMapper}.
 *
 * Implementations have to define how contexts are parsed at query/index time
 */
public abstract class ContextMapping<T extends ToXContent> implements ToXContentFragment {

    public static final String FIELD_TYPE = "type";
    public static final String FIELD_NAME = "name";
    protected final Type type;
    protected final String name;

    public enum Type {
        CATEGORY,
        GEO;

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
    public abstract Set<String> parseContext(DocumentParserContext documentParserContext, XContentParser parser) throws IOException,
        ElasticsearchParseException;

    /**
     * Retrieves a set of context from a <code>document</code> at index-time.
     */
    protected abstract Set<String> parseContext(LuceneDocument document);

    /**
     * Prototype for the query context
     */
    protected abstract T fromXContent(XContentParser context) throws IOException;

    /**
     * Parses query contexts for this mapper
     */
    public final List<InternalQueryContext> parseQueryContext(XContentParser parser) throws IOException, ElasticsearchParseException {
        List<T> queryContexts = new ArrayList<>();
        Token token = parser.nextToken();
        if (token == Token.START_ARRAY) {
            while (parser.nextToken() != Token.END_ARRAY) {
                queryContexts.add(fromXContent(parser));
            }
        } else {
            queryContexts.add(fromXContent(parser));
        }

        return toInternalQueryContexts(queryContexts);
    }

    /**
     * Convert query contexts to common representation
     */
    protected abstract List<InternalQueryContext> toInternalQueryContexts(List<T> queryContexts);

    /**
     * Implementations should add specific configurations
     * that need to be persisted
     */
    protected abstract XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Checks if the current context is consistent with the rest of the fields. For example, the GeoContext
     * should check that the field that it points to has the correct type.
     */
    public void validateReferences(Version indexVersionCreated, Function<String, MappedFieldType> fieldResolver) {
        // No validation is required by default
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_NAME, name);
        builder.field(FIELD_TYPE, type.name());
        toInnerXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContextMapping<?> that = (ContextMapping<?>) o;
        if (type != that.type) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    @Override
    public String toString() {
        try {
            return Strings.toString(toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));
        } catch (IOException e) {
            return super.toString();
        }
    }

    public static class InternalQueryContext {
        public final String context;
        public final int boost;
        public final boolean isPrefix;

        public InternalQueryContext(String context, int boost, boolean isPrefix) {
            this.context = context;
            this.boost = boost;
            this.isPrefix = isPrefix;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            InternalQueryContext that = (InternalQueryContext) o;

            if (boost != that.boost) return false;
            if (isPrefix != that.isPrefix) return false;
            return context != null ? context.equals(that.context) : that.context == null;

        }

        @Override
        public int hashCode() {
            int result = context != null ? context.hashCode() : 0;
            result = 31 * result + boost;
            result = 31 * result + (isPrefix ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "QueryContext{" + "context='" + context + '\'' + ", boost=" + boost + ", isPrefix=" + isPrefix + '}';
        }
    }
}
