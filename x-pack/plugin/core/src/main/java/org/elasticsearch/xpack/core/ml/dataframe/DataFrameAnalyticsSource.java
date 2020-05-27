/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameAnalyticsSource implements Writeable, ToXContentObject {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField _SOURCE = new ParseField("_source");

    public static ConstructingObjectParser<DataFrameAnalyticsSource, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DataFrameAnalyticsSource, Void> parser = new ConstructingObjectParser<>("data_frame_analytics_source",
            ignoreUnknownFields, a -> new DataFrameAnalyticsSource(
                ((List<String>) a[0]).toArray(new String[0]),
                (QueryProvider) a[1],
                (FetchSourceContext) a[2]));
        parser.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> QueryProvider.fromXContent(p, ignoreUnknownFields, Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT), QUERY);
        parser.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> FetchSourceContext.fromXContent(p),
            _SOURCE,
            ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING);
        return parser;
    }

    private final String[] index;
    private final QueryProvider queryProvider;
    private final FetchSourceContext sourceFiltering;

    public DataFrameAnalyticsSource(String[] index, @Nullable QueryProvider queryProvider, @Nullable FetchSourceContext sourceFiltering) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX);
        if (index.length == 0) {
            throw new IllegalArgumentException("source.index must specify at least one index");
        }
        if (Arrays.stream(index).anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("source.index must contain non-null and non-empty strings");
        }
        this.queryProvider = queryProvider == null ? QueryProvider.defaultQuery() : queryProvider;
        if (sourceFiltering != null && sourceFiltering.fetchSource() == false) {
            throw new IllegalArgumentException("source._source cannot be disabled");
        }
        this.sourceFiltering = sourceFiltering;
    }

    public DataFrameAnalyticsSource(StreamInput in) throws IOException {
        index = in.readStringArray();
        queryProvider = QueryProvider.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            sourceFiltering = in.readOptionalWriteable(FetchSourceContext::new);
        } else {
            sourceFiltering = null;
        }
    }

    public DataFrameAnalyticsSource(DataFrameAnalyticsSource other) {
        this.index = Arrays.copyOf(other.index, other.index.length);
        this.queryProvider = new QueryProvider(other.queryProvider);
        this.sourceFiltering = other.sourceFiltering == null ? null : new FetchSourceContext(
            other.sourceFiltering.fetchSource(), other.sourceFiltering.includes(), other.sourceFiltering.excludes());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(index);
        queryProvider.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeOptionalWriteable(sourceFiltering);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(INDEX.getPreferredName(), index);
        builder.field(QUERY.getPreferredName(), queryProvider.getQuery());
        if (sourceFiltering != null) {
            builder.field(_SOURCE.getPreferredName(), sourceFiltering);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsSource other = (DataFrameAnalyticsSource) o;
        return Arrays.equals(index, other.index)
            && Objects.equals(queryProvider, other.queryProvider)
            && Objects.equals(sourceFiltering, other.sourceFiltering);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.asList(index), queryProvider, sourceFiltering);
    }

    public String[] getIndex() {
        return index;
    }

    /**
     * Get the fully parsed query from the semi-parsed stored {@code Map<String, Object>}
     *
     * @return Fully parsed query
     */
    public QueryBuilder getParsedQuery() {
        Exception exception = queryProvider.getParsingException();
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else {
                throw new ElasticsearchException(queryProvider.getParsingException());
            }
        }
        return queryProvider.getParsedQuery();
    }

    public FetchSourceContext getSourceFiltering() {
        return sourceFiltering;
    }

    Exception getQueryParsingException() {
        return queryProvider.getParsingException();
    }

    // visible for testing
    QueryProvider getQueryProvider() {
        return queryProvider;
    }

    /**
     * Calls the parser and returns any gathered deprecations
     *
     * @param namedXContentRegistry XContent registry to transform the lazily parsed query
     * @return The deprecations from parsing the query
     */
    public List<String> getQueryDeprecations(NamedXContentRegistry namedXContentRegistry) {
        List<String> deprecations = new ArrayList<>();
        try {
            XContentObjectTransformer.queryBuilderTransformer(namedXContentRegistry).fromMap(queryProvider.getQuery(),
                deprecations);
        } catch (Exception exception) {
            // Certain thrown exceptions wrap up the real Illegal argument making it hard to determine cause for the user
            if (exception.getCause() instanceof IllegalArgumentException) {
                exception = (Exception) exception.getCause();
            }
            throw ExceptionsHelper.badRequestException(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, exception);
        }
        return deprecations;
    }

    // Visible for testing
    Map<String, Object> getQuery() {
        return queryProvider.getQuery();
    }

    public boolean isFieldExcluded(String path) {
        if (sourceFiltering == null) {
            return false;
        }

        // First we check in the excludes as they are applied last
        for (String exclude : sourceFiltering.excludes()) {
            if (pathMatchesSourcePattern(path, exclude)) {
                return true;
            }
        }

        // Now we can check the includes

        // Empty includes means no further exclusions
        if (sourceFiltering.includes().length == 0) {
            return false;
        }

        for (String include : sourceFiltering.includes()) {
            if (pathMatchesSourcePattern(path, include)) {
                return false;
            }
        }
        return true;
    }

    private static boolean pathMatchesSourcePattern(String path, String sourcePattern) {
        if (sourcePattern.equals(path)) {
            return true;
        }

        if (Regex.isSimpleMatchPattern(sourcePattern)) {
            return Regex.simpleMatch(sourcePattern, path);
        }

        // At this stage sourcePattern is a concrete field name and path is not equal to it.
        // We should check if path is a nested field of pattern.
        // Let us take "foo" as an example.
        // Fields that are "foo.*" should also be matched.
        return Regex.simpleMatch(sourcePattern + ".*", path);
    }
}
