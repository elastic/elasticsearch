/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.ScriptedSimilarity;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.DATAFEED_CONFIG_QUERY_BAD_FORMAT;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_CONFIG_QUERY_BAD_FORMAT;
import static org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper.requireNonNull;

public record QueryExtractorBuilder(String featureName, QueryProvider query) implements LearnToRankFeatureExtractorBuilder {

    public static final ParseField NAME = new ParseField("named_query");
    public static final ParseField FEATURE_NAME = new ParseField("feature_name");
    public static final ParseField QUERY = new ParseField("query");

    private static final ConstructingObjectParser<QueryExtractorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new QueryExtractorBuilder((String) a[0], (QueryProvider) a[1])
    );
    private static final ConstructingObjectParser<QueryExtractorBuilder, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        true,
        a -> new QueryExtractorBuilder((String) a[0], (QueryProvider) a[1])
    );
    static {
        PARSER.declareString(constructorArg(), FEATURE_NAME);
        PARSER.declareObject(
            constructorArg(),
            (p, c) -> QueryProvider.fromXContent(p, false, INFERENCE_CONFIG_QUERY_BAD_FORMAT),
            QUERY
        );
        LENIENT_PARSER.declareString(constructorArg(), FEATURE_NAME);
        LENIENT_PARSER.declareObject(
            constructorArg(),
            (p, c) -> QueryProvider.fromXContent(p, true, INFERENCE_CONFIG_QUERY_BAD_FORMAT),
            QUERY
        );
    }

    public static QueryExtractorBuilder fromXContent(XContentParser parser, Object context) {
        boolean lenient = Boolean.TRUE.equals(context);
        return lenient ? LENIENT_PARSER.apply(parser, null) : PARSER.apply(parser, null);
    }

    public QueryExtractorBuilder(String featureName, QueryProvider query) {
        this.featureName = requireNonNull(featureName, FEATURE_NAME);
        this.query = requireNonNull(query, QUERY);
    }

    public QueryExtractorBuilder(StreamInput input) throws IOException {
        this(input.readString(), QueryProvider.fromStream(input));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAME.getPreferredName(), featureName);
        builder.field(QUERY.getPreferredName(), query.getQuery());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(featureName);
        query.writeTo(out);
    }

    @Override
    public String featureName() {
        return featureName;
    }

    @Override
    public void validate() throws Exception {
        if (query.getParsingException() != null) {
            throw query.getParsingException();
        }
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryExtractorBuilder that = (QueryExtractorBuilder) o;
        return Objects.equals(featureName, that.featureName) && Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureName, query);
    }

    @Override
    public QueryExtractorBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        QueryProvider rewritten = query.rewrite(ctx);
        if (rewritten == query) {
            return this;
        }
        return new QueryExtractorBuilder(featureName, query);
    }

    @Override
    public ParsedQuery parsedQuery(SearchExecutionContext executionContext) {
        return executionContext.toQuery(query.getParsedQuery());
    }
}
