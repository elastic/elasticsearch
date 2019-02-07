/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameAnalyticsConfig implements ToXContentObject, Writeable {

    public static final String TYPE = "data_frame_analytics_config";

    private static final XContentObjectTransformer<QueryBuilder> QUERY_TRANSFORMER = XContentObjectTransformer.queryBuilderTransformer();
    static final TriFunction<Map<String, Object>, String, List<String>, QueryBuilder> lazyQueryParser =
        (objectMap, id, warnings) -> {
            try {
                return QUERY_TRANSFORMER.fromMap(objectMap, warnings);
            } catch (IOException | XContentParseException exception) {
                // Certain thrown exceptions wrap up the real Illegal argument making it hard to determine cause for the user
                if (exception.getCause() instanceof IllegalArgumentException) {
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT,
                            id,
                            exception.getCause().getMessage()),
                        exception.getCause());
                } else {
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, exception, id),
                        exception);
                }
            }
        };


    public static final ParseField ID = new ParseField("id");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DEST = new ParseField("dest");
    public static final ParseField ANALYSES = new ParseField("analyses");
    public static final ParseField CONFIG_TYPE = new ParseField("config_type");
    public static final ParseField QUERY = new ParseField("query");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    public static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(TYPE, ignoreUnknownFields, Builder::new);

        parser.declareString((c, s) -> {}, CONFIG_TYPE);
        parser.declareString(Builder::setId, ID);
        parser.declareString(Builder::setSource, SOURCE);
        parser.declareString(Builder::setDest, DEST);
        parser.declareObjectArray(Builder::setAnalyses, DataFrameAnalysisConfig.parser(), ANALYSES);
        if (ignoreUnknownFields) {
            parser.declareObject(Builder::setQuery, (p, c) -> p.mapOrdered(), QUERY);
        } else {
            parser.declareObject(Builder::setParsedQuery, (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), QUERY);
        }
        return parser;
    }

    private final String id;
    private final String source;
    private final String dest;
    private final List<DataFrameAnalysisConfig> analyses;
    private final Map<String, Object> query;
    private final CachedSupplier<QueryBuilder> querySupplier;

    public DataFrameAnalyticsConfig(String id, String source, String dest, List<DataFrameAnalysisConfig> analyses,
                                    Map<String, Object> query) {
        this.id = ExceptionsHelper.requireNonNull(id, ID);
        this.source = ExceptionsHelper.requireNonNull(source, SOURCE);
        this.dest = ExceptionsHelper.requireNonNull(dest, DEST);
        this.analyses = ExceptionsHelper.requireNonNull(analyses, ANALYSES);
        if (analyses.isEmpty()) {
            throw new ElasticsearchParseException("One or more analyses are required");
        }
        // TODO Add support for multiple analyses
        if (analyses.size() > 1) {
            throw new UnsupportedOperationException("Does not yet support multiple analyses");
        }
        this.query = query == null ? null : Collections.unmodifiableMap(query);
        this.querySupplier = new CachedSupplier<>(() -> lazyQueryParser.apply(query, id, new ArrayList<>()));
    }

    public DataFrameAnalyticsConfig(StreamInput in) throws IOException {
        id = in.readString();
        source = in.readString();
        dest = in.readString();
        analyses = in.readList(DataFrameAnalysisConfig::new);
        if (in.readBoolean()) {
            this.query = in.readMap();
        } else {
            this.query = null;
        }
        this.querySupplier = new CachedSupplier<>(() -> lazyQueryParser.apply(query, id, new ArrayList<>()));
    }

    public String getId() {
        return id;
    }

    public String getSource() {
        return source;
    }

    public String getDest() {
        return dest;
    }

    public List<DataFrameAnalysisConfig> getAnalyses() {
        return analyses;
    }

    @Nullable
    public Map<String, Object> getQuery() {
        return query;
    }

    @Nullable
    public QueryBuilder getParsedQuery() {
        return querySupplier.get();
    }

    /**
     * Calls the lazy parser and returns any gathered deprecations
     * @return The deprecations from parsing the query
     */
    public List<String> getQueryDeprecations() {
        return getQueryDeprecations(lazyQueryParser);
    }

    List<String> getQueryDeprecations(TriFunction<Map<String, Object>, String, List<String>, QueryBuilder> parser) {
        List<String> deprecations = new ArrayList<>();
        parser.apply(query, id, deprecations);
        return deprecations;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(SOURCE.getPreferredName(), source);
        builder.field(DEST.getPreferredName(), dest);
        builder.field(ANALYSES.getPreferredName(), analyses);
        if (params.paramAsBoolean(ToXContentParams.INCLUDE_TYPE, false)) {
            builder.field(CONFIG_TYPE.getPreferredName(), TYPE);
        }
        if (query != null) {
            builder.field(QUERY.getPreferredName(), query);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(source);
        out.writeString(dest);
        out.writeList(analyses);
        out.writeBoolean(query != null);
        if (query != null) {
            out.writeMap(query);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsConfig other = (DataFrameAnalyticsConfig) o;
        return Objects.equals(id, other.id)
            && Objects.equals(source, other.source)
            && Objects.equals(dest, other.dest)
            && Objects.equals(analyses, other.analyses)
            && Objects.equals(query, other.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, analyses, query);
    }

    public static String documentId(String id) {
        return TYPE + "-" + id;
    }

    public static class Builder {

        private String id;
        private String source;
        private String dest;
        private List<DataFrameAnalysisConfig> analyses;
        private Map<String, Object> query;

        public String getId() {
            return id;
        }

        public Builder setId(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, ID);
            return this;
        }

        public Builder setSource(String source) {
            this.source = ExceptionsHelper.requireNonNull(source, SOURCE);
            return this;
        }

        public Builder setDest(String dest) {
            this.dest = ExceptionsHelper.requireNonNull(dest, DEST);
            return this;
        }

        public Builder setAnalyses(List<DataFrameAnalysisConfig> analyses) {
            this.analyses = ExceptionsHelper.requireNonNull(analyses, ANALYSES);
            return this;
        }

        public Builder setParsedQuery(QueryBuilder query) {
            try {
                setQuery(QUERY_TRANSFORMER.toMap(query));
                return this;
            } catch (IOException exception) { // This exception should never really throw as we are simply transforming the object to a map
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, id, exception.getMessage()), exception);
            }
        }

        public Builder setQuery(Map<String, Object> query) {
            this.query = query;
            return this;
        }

        public DataFrameAnalyticsConfig build() {
            return new DataFrameAnalyticsConfig(id, source, dest, analyses, query);
        }
    }
}
