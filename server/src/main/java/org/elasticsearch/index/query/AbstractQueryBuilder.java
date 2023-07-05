/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.FilterXContentParser;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.search.SearchModule.INDICES_MAX_NESTED_DEPTH_SETTING;

/**
 * Base class for all classes producing lucene queries.
 * Supports conversion to BytesReference and creation of lucene Query objects.
 */
public abstract class AbstractQueryBuilder<QB extends AbstractQueryBuilder<QB>> implements QueryBuilder {

    /** Default for boost to apply to resulting Lucene query. Defaults to 1.0*/
    public static final float DEFAULT_BOOST = 1.0f;
    public static final ParseField NAME_FIELD = new ParseField("_name");
    public static final ParseField BOOST_FIELD = new ParseField("boost");
    // We set the default value for tests that don't go through SearchModule
    private static int maxNestedDepth = INDICES_MAX_NESTED_DEPTH_SETTING.getDefault(Settings.EMPTY);

    protected String queryName;
    protected float boost = DEFAULT_BOOST;

    protected AbstractQueryBuilder() {

    }

    protected AbstractQueryBuilder(StreamInput in) throws IOException {
        boost = in.readFloat();
        checkNegativeBoost(boost);
        queryName = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(boost);
        out.writeOptionalString(queryName);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Add {@code boost} and {@code query_name} to the builder.
     * @deprecated use {@link #boostAndQueryNameToXContent}
     */
    @Deprecated
    protected final void printBoostAndQueryName(XContentBuilder builder) throws IOException {
        builder.field(BOOST_FIELD.getPreferredName(), boost);
        if (queryName != null) {
            builder.field(NAME_FIELD.getPreferredName(), queryName);
        }
    }

    /**
     * Add {@code boost} and {@code query_name} to the builder.
     */
    protected final void boostAndQueryNameToXContent(XContentBuilder builder) throws IOException {
        if (boost != DEFAULT_BOOST) {
            builder.field(BOOST_FIELD.getPreferredName(), boost);
        }
        if (queryName != null) {
            builder.field(NAME_FIELD.getPreferredName(), queryName);
        }
    }

    @Override
    public final Query toQuery(SearchExecutionContext context) throws IOException {
        Query query = doToQuery(context);
        if (query != null) {
            if (boost != DEFAULT_BOOST) {
                if (query instanceof MatchNoDocsQuery == false) {
                    query = new BoostQuery(query, boost);
                }
            }
            if (queryName != null) {
                context.addNamedQuery(queryName, query);
            }
        }
        return query;
    }

    protected abstract Query doToQuery(SearchExecutionContext context) throws IOException;

    /**
     * Sets the query name for the query.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final QB queryName(String queryName) {
        this.queryName = queryName;
        return (QB) this;
    }

    /**
     * Returns the query name for the query.
     */
    @Override
    public final String queryName() {
        return queryName;
    }

    /**
     * Returns the boost for this query.
     */
    @Override
    public final float boost() {
        return this.boost;
    }

    protected final void checkNegativeBoost(float boost) {
        if (Float.compare(boost, 0f) < 0) {
            throw new IllegalArgumentException(
                "negative [boost] are not allowed in [" + toString() + "], use a value between 0 and 1 to deboost"
            );
        }
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final QB boost(float boost) {
        checkNegativeBoost(boost);
        this.boost = boost;
        return (QB) this;
    }

    protected final QueryValidationException addValidationError(String validationError, QueryValidationException validationException) {
        return QueryValidationException.addValidationError(getName(), validationError, validationException);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        QB other = (QB) obj;
        return Objects.equals(queryName, other.queryName) && Objects.equals(boost, other.boost) && doEquals(other);
    }

    /**
     * Indicates whether some other {@link QueryBuilder} object of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(QB other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), queryName, boost, doHashCode());
    }

    protected abstract int doHashCode();

    /**
     * This helper method checks if the object passed in is a string or {@link CharBuffer},
     * if so it converts it to a {@link BytesRef}.
     * @param obj the input object
     * @return the same input object or a {@link BytesRef} representation if input was of type string
     */
    static Object maybeConvertToBytesRef(Object obj) {
        if (obj instanceof String) {
            return BytesRefs.toBytesRef(obj);
        } else if (obj instanceof CharBuffer) {
            return new BytesRef((CharBuffer) obj);
        } else if (obj instanceof BigInteger) {
            return BytesRefs.toBytesRef(obj);
        }
        return obj;
    }

    /**
     * This helper method checks if the object passed in is a {@link BytesRef} or {@link CharBuffer},
     * if so it converts it to a utf8 string.
     * @param obj the input object
     * @return the same input object or a utf8 string if input was of type {@link BytesRef} or {@link CharBuffer}
     */
    static Object maybeConvertToString(Object obj) {
        if (obj instanceof BytesRef) {
            return ((BytesRef) obj).utf8ToString();
        } else if (obj instanceof CharBuffer) {
            return new BytesRef((CharBuffer) obj).utf8ToString();
        }
        return obj;
    }

    /**
     * Helper method to convert collection of {@link QueryBuilder} instances to lucene
     * {@link Query} instances. {@link QueryBuilder} that return {@code null} calling
     * their {@link QueryBuilder#toQuery(SearchExecutionContext)} method are not added to the
     * resulting collection.
     */
    static Collection<Query> toQueries(Collection<QueryBuilder> queryBuilders, SearchExecutionContext context) throws QueryShardException,
        IOException {
        List<Query> queries = new ArrayList<>(queryBuilders.size());
        for (QueryBuilder queryBuilder : queryBuilders) {
            Query query = queryBuilder.rewrite(context).toQuery(context);
            if (query != null) {
                queries.add(query);
            }
        }
        return queries;
    }

    @Override
    public String getName() {
        // default impl returns the same as writeable name, but we keep the distinction between the two just to make sure
        return getWriteableName();
    }

    protected static void writeQueries(StreamOutput out, List<? extends QueryBuilder> queries) throws IOException {
        out.writeVInt(queries.size());
        for (QueryBuilder query : queries) {
            out.writeNamedWriteable(query);
        }
    }

    protected static List<QueryBuilder> readQueries(StreamInput in) throws IOException {
        return in.readNamedWriteableList(QueryBuilder.class);
    }

    @Override
    public final QueryBuilder rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder rewritten = doRewrite(queryRewriteContext);
        if (rewritten == this) {
            return rewritten;
        }
        if (queryName() != null && rewritten.queryName() == null) { // we inherit the name
            rewritten.queryName(queryName());
        }
        if (boost() != DEFAULT_BOOST && rewritten.boost() == DEFAULT_BOOST) {
            rewritten.boost(boost());
        }
        return rewritten;
    }

    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        // NOTE: sometimes rewrites are attempted after calling `QueryRewriteContext#convertToSearchExecutionContext`
        // which returns `null` in the default implementation.
        if (queryRewriteContext == null) {
            return this;
        }
        final CoordinatorRewriteContext crc = queryRewriteContext.convertToCoordinatorRewriteContext();
        if (crc != null) {
            return doCoordinatorRewrite(crc);
        }
        final SearchExecutionContext sec = queryRewriteContext.convertToSearchExecutionContext();
        if (sec != null) {
            return doSearchRewrite(sec);
        }
        final QueryRewriteContext context = queryRewriteContext.convertToIndexMetadataContext();
        if (context != null) {
            return doIndexMetadataRewrite(context);
        }
        return this;
    }

    /**
     * @param coordinatorRewriteContext A {@link QueryRewriteContext} that enables limited rewrite capabilities
     *                                  happening on the coordinator node before execution moves to the data node.
     * @return A {@link QueryBuilder} representing the rewritten query which could be executed without going to
     * the date node.
     */
    protected QueryBuilder doCoordinatorRewrite(final CoordinatorRewriteContext coordinatorRewriteContext) {
        return this;
    }

    /**
     * @param searchExecutionContext A {@link QueryRewriteContext} that enables full rewrite capabilities
     *                               happening on the data node with all information available for rewriting.
     * @return A {@link QueryBuilder} representing the rewritten query.
     */
    protected QueryBuilder doSearchRewrite(final SearchExecutionContext searchExecutionContext) throws IOException {
        return doIndexMetadataRewrite(searchExecutionContext);
    }

    /**
     * Optional rewrite logic that only needs access to index level metadata and services (e.g. index settings and mappings)
     * on the data node, but not the shard / Lucene index.
     * The can_match phase can use this logic to early terminate a search without doing any search related i/o.
     *
     * @param context an {@link QueryRewriteContext} instance that has access the mappings and other index metadata
     * @return A {@link QueryBuilder} representing the rewritten query, that could be used to determine whether this query yields result.
     */
    protected QueryBuilder doIndexMetadataRewrite(final QueryRewriteContext context) throws IOException {
        return this;
    }

    /**
     * For internal usage only!
     *
     * Extracts the inner hits from the query tree.
     * While it extracts inner hits, child inner hits are inlined into the inner hit builder they belong to.
     */
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {}

    /**
     * Parses and returns a query (excluding the query field that wraps it). To be called by API that support
     * user provided queries. Note that the returned query may hold inner queries, and so on. Calling this method
     * will initialize the tracking of nested depth to make sure that there's a limit to the number of queries
     * that can be nested within one another (see {@link org.elasticsearch.search.SearchModule#INDICES_MAX_NESTED_DEPTH_SETTING}.
     * This variant of the method does not support collecting statistics about queries usage.
     */
    public static QueryBuilder parseTopLevelQuery(XContentParser parser) throws IOException {
        return parseTopLevelQuery(parser, queryName -> {});
    }

    /**
     * Parses and returns a query (excluding the query field that wraps it). To be called by API that support
     * user provided queries. Note that the returned query may hold inner queries, and so on. Calling this method
     * will initialize the tracking of nested depth to make sure that there's a limit to the number of queries
     * that can be nested within one another (see {@link org.elasticsearch.search.SearchModule#INDICES_MAX_NESTED_DEPTH_SETTING}.
     * The method accepts a string consumer that will be provided with each query type used in the parsed content, to be used
     * for instance to collect statistics about queries usage.
     */
    public static QueryBuilder parseTopLevelQuery(XContentParser parser, Consumer<String> queryNameConsumer) throws IOException {
        FilterXContentParser parserWrapper = new FilterXContentParserWrapper(parser) {
            int nestedDepth;

            @Override
            public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
                if (categoryClass.equals(QueryBuilder.class)) {
                    nestedDepth++;
                    if (nestedDepth > maxNestedDepth) {
                        throw new IllegalArgumentException(
                            "The nested depth of the query exceeds the maximum nested depth for queries set in ["
                                + INDICES_MAX_NESTED_DEPTH_SETTING.getKey()
                                + "]"
                        );
                    }
                }
                T namedObject = getXContentRegistry().parseNamedObject(categoryClass, name, this, context);
                if (categoryClass.equals(QueryBuilder.class)) {
                    queryNameConsumer.accept(name);
                    nestedDepth--;
                }
                return namedObject;
            }
        };
        return parseInnerQueryBuilder(parserWrapper);
    }

    /**
     * Parses an inner query. To be called by query implementations that support inner queries.
     */
    protected static QueryBuilder parseInnerQueryBuilder(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, must start with start_object");
            }
        }
        if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
            // we encountered '{}' for a query clause, it used to be supported, deprecated in 5.0 and removed in 6.0
            throw new IllegalArgumentException("query malformed, empty clause found at [" + parser.getTokenLocation() + "]");
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[_na] query malformed, no field after start_object");
        }
        String queryName = parser.currentName();
        // move to the next START_OBJECT
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + queryName + "] query malformed, no start_object after query name");
        }
        QueryBuilder result;
        try {
            result = parser.namedObject(QueryBuilder.class, queryName, null);
        } catch (NamedObjectNotFoundException e) {
            String message = String.format(
                Locale.ROOT,
                "unknown query [%s]%s",
                queryName,
                SuggestingErrorOnUnknown.suggest(queryName, e.getCandidates())
            );
            throw new ParsingException(new XContentLocation(e.getLineNumber(), e.getColumnNumber()), message, e);
        }
        // end_object of the specific query (e.g. match, multi_match etc.) element
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]"
            );
        }
        // end_object of the query object
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + queryName + "] malformed query, expected [END_OBJECT] but found [" + parser.currentToken() + "]"
            );
        }
        return result;
    }

    // Like Objects.requireNotNull(...) but instead throws a IllegalArgumentException
    protected static <T> T requireValue(T value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    protected static void throwParsingExceptionOnMultipleFields(
        String queryName,
        XContentLocation contentLocation,
        String processedFieldName,
        String currentFieldName
    ) {
        if (processedFieldName != null) {
            throw new ParsingException(
                contentLocation,
                "["
                    + queryName
                    + "] query doesn't support multiple fields, found ["
                    + processedFieldName
                    + "] and ["
                    + currentFieldName
                    + "]"
            );
        }
    }

    /**
     * Adds {@code boost} and {@code query_name} parsing to the
     * {@link AbstractObjectParser} passed in. All query builders except
     * {@link MatchAllQueryBuilder} and {@link MatchNoneQueryBuilder} support these fields so they
     * should use this method.
     */
    protected static void declareStandardFields(AbstractObjectParser<? extends QueryBuilder, ?> parser) {
        parser.declareFloat(QueryBuilder::boost, AbstractQueryBuilder.BOOST_FIELD);
        parser.declareString(QueryBuilder::queryName, AbstractQueryBuilder.NAME_FIELD);
    }

    /**
     * Set the maximum nested depth of bool queries.
     * Default value is 20.
     */
    public static void setMaxNestedDepth(int maxNestedDepth) {
        if (maxNestedDepth < 1) {
            throw new IllegalArgumentException("maxNestedDepth must be >= 1");
        }
        AbstractQueryBuilder.maxNestedDepth = maxNestedDepth;
    }

    public static int getMaxNestedDepth() {
        return maxNestedDepth;
    }

    @Override
    public final String toString() {
        return Strings.toString(this, true, true);
    }
}
