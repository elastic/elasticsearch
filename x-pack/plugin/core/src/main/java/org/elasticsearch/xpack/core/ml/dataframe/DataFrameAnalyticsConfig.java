/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING;
import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.VALUE;

public class DataFrameAnalyticsConfig implements ToXContentObject, Writeable {

    private static final Logger logger = LogManager.getLogger(DataFrameAnalyticsConfig.class);
    public static final String TYPE = "data_frame_analytics_config";

    public static final ByteSizeValue DEFAULT_MODEL_MEMORY_LIMIT = new ByteSizeValue(1, ByteSizeUnit.GB);
    public static final ByteSizeValue MIN_MODEL_MEMORY_LIMIT = new ByteSizeValue(1, ByteSizeUnit.MB);

    private static final XContentObjectTransformer<QueryBuilder> QUERY_TRANSFORMER = XContentObjectTransformer.queryBuilderTransformer();
    static final TriFunction<Map<String, Object>, String, List<String>, QueryBuilder> lazyQueryParser =
        (objectMap, id, warnings) -> {
            try {
                return QUERY_TRANSFORMER.fromMap(objectMap, warnings);
            } catch (IOException | XContentParseException exception) {
                // Certain thrown exceptions wrap up the real Illegal argument making it hard to determine cause for the user
                if (exception.getCause() instanceof IllegalArgumentException) {
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, id), exception.getCause());
                } else {
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, id), exception);
                }
            }
        };


    public static final ParseField ID = new ParseField("id");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DEST = new ParseField("dest");
    public static final ParseField ANALYSES = new ParseField("analyses");
    public static final ParseField CONFIG_TYPE = new ParseField("config_type");
    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField ANALYSES_FIELDS = new ParseField("analyses_fields");
    public static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    public static final ParseField HEADERS = new ParseField("headers");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    public static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(TYPE, ignoreUnknownFields, Builder::new);

        parser.declareString((c, s) -> {}, CONFIG_TYPE);
        parser.declareString(Builder::setId, ID);
        parser.declareString(Builder::setSource, SOURCE);
        parser.declareString(Builder::setDest, DEST);
        parser.declareObjectArray(Builder::setAnalyses, DataFrameAnalysisConfig.parser(), ANALYSES);
        parser.declareObject((builder, query) -> builder.setQuery(query, ignoreUnknownFields), (p, c) -> p.mapOrdered(), QUERY);
        parser.declareField(Builder::setAnalysesFields,
            (p, c) -> FetchSourceContext.fromXContent(p),
            ANALYSES_FIELDS,
            OBJECT_ARRAY_BOOLEAN_OR_STRING);
        parser.declareField(Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()), MODEL_MEMORY_LIMIT, VALUE);
        if (ignoreUnknownFields) {
            // Headers are not parsed by the strict (config) parser, so headers supplied in the _body_ of a REST request will be rejected.
            // (For config, headers are explicitly transferred from the auth headers by code in the put data frame actions.)
            parser.declareObject(Builder::setHeaders, (p, c) -> p.mapStrings(), HEADERS);
        }
        return parser;
    }

    private final String id;
    private final String source;
    private final String dest;
    private final List<DataFrameAnalysisConfig> analyses;
    private final Map<String, Object> query;
    private final CachedSupplier<QueryBuilder> querySupplier;
    private final FetchSourceContext analysesFields;
    private final ByteSizeValue modelMemoryLimit;
    private final Map<String, String> headers;

    public DataFrameAnalyticsConfig(String id, String source, String dest, List<DataFrameAnalysisConfig> analyses,
                                    Map<String, Object> query, Map<String, String> headers, ByteSizeValue modelMemoryLimit,
                                    FetchSourceContext analysesFields) {
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
        this.query = Collections.unmodifiableMap(query);
        this.querySupplier = new CachedSupplier<>(() -> lazyQueryParser.apply(query, id, new ArrayList<>()));
        this.analysesFields = analysesFields;
        this.modelMemoryLimit = modelMemoryLimit;
        this.headers = Collections.unmodifiableMap(headers);
    }

    public DataFrameAnalyticsConfig(StreamInput in) throws IOException {
        id = in.readString();
        source = in.readString();
        dest = in.readString();
        analyses = in.readList(DataFrameAnalysisConfig::new);
        this.query = in.readMap();
        this.querySupplier = new CachedSupplier<>(() -> lazyQueryParser.apply(query, id, new ArrayList<>()));
        this.analysesFields = in.readOptionalWriteable(FetchSourceContext::new);
        this.modelMemoryLimit = in.readBoolean() ? new ByteSizeValue(in) : null;
        this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
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

    public FetchSourceContext getAnalysesFields() {
        return analysesFields;
    }

    /**
     * Calls the lazy parser and returns any gathered deprecations
     * @return The deprecations from parsing the query
     */
    List<String> getQueryDeprecations() {
        return getQueryDeprecations(lazyQueryParser);
    }

    List<String> getQueryDeprecations(TriFunction<Map<String, Object>, String, List<String>, QueryBuilder> parser) {
        List<String> deprecations = new ArrayList<>();
        parser.apply(query, id, deprecations);
        return deprecations;
    }

    public ByteSizeValue getModelMemoryLimit() {
        return modelMemoryLimit != null ? modelMemoryLimit : DEFAULT_MODEL_MEMORY_LIMIT;
    }

    public Map<String, String> getHeaders() {
        return headers;
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
        builder.field(QUERY.getPreferredName(), query);
        if (analysesFields != null) {
            builder.field(ANALYSES_FIELDS.getPreferredName(), analysesFields);
        }
        if (modelMemoryLimit != null) {
            builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit.getStringRep());
        }
        if (headers.isEmpty() == false && params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(HEADERS.getPreferredName(), headers);
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
        out.writeMap(query);
        out.writeOptionalWriteable(analysesFields);
        if (modelMemoryLimit != null) {
            out.writeBoolean(true);
            modelMemoryLimit.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
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
            && Objects.equals(query, other.query)
            && Objects.equals(headers, other.headers)
            && Objects.equals(modelMemoryLimit, other.modelMemoryLimit)
            && Objects.equals(analysesFields, other.analysesFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, analyses, query, headers, modelMemoryLimit, analysesFields);
    }

    public static String documentId(String id) {
        return TYPE + "-" + id;
    }

    public static class Builder {

        private String id;
        private String source;
        private String dest;
        private List<DataFrameAnalysisConfig> analyses;
        private Map<String, Object> query = Collections.singletonMap(MatchAllQueryBuilder.NAME, Collections.emptyMap());
        private FetchSourceContext analysesFields;
        private ByteSizeValue modelMemoryLimit;
        private Map<String, String> headers = Collections.emptyMap();

        public Builder() {}

        public Builder(String id) {
            setId(id);
        }

        public String getId() {
            return id;
        }

        public Builder(DataFrameAnalyticsConfig config) {
            this.id = config.id;
            this.source = config.source;
            this.dest = config.dest;
            this.analyses = new ArrayList<>(config.analyses);
            this.query = new LinkedHashMap<>(config.query);
            this.headers = new HashMap<>(config.headers);
            this.modelMemoryLimit = config.modelMemoryLimit;
            if (config.analysesFields != null) {
               this.analysesFields = new FetchSourceContext(true, config.analysesFields.includes(), config.analysesFields.excludes());
            }
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

        public Builder setQuery(Map<String, Object> query) {
            return setQuery(query, true);
        }

        public Builder setQuery(Map<String, Object> query, boolean lenient) {
            this.query = ExceptionsHelper.requireNonNull(query, QUERY.getPreferredName());
            try {
                QUERY_TRANSFORMER.fromMap(query);
            } catch (Exception exception) {
                if (lenient) {
                    logger.warn(Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, id), exception);
                } else {
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT, id), exception);
                }
            }
            return this;
        }

        public Builder setAnalysesFields(FetchSourceContext fields) {
            this.analysesFields = fields;
            return this;
        }

        public Builder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder setModelMemoryLimit(ByteSizeValue modelMemoryLimit) {
            if (modelMemoryLimit != null && modelMemoryLimit.compareTo(MIN_MODEL_MEMORY_LIMIT) < 0) {
                throw new IllegalArgumentException("[" + MODEL_MEMORY_LIMIT.getPreferredName()
                    + "] must be at least [" + MIN_MODEL_MEMORY_LIMIT.getStringRep() + "].");
            }
            this.modelMemoryLimit = modelMemoryLimit;
            return this;
        }

        public DataFrameAnalyticsConfig applyMaxModelMemoryLimitAndBuild(ByteSizeValue maxModelMemoryLimit) {

            boolean maxModelMemoryIsSet = maxModelMemoryLimit != null && maxModelMemoryLimit.getMb() > 0;

            if (modelMemoryLimit == null) {
                // Default is silently capped if higher than limit
                if (maxModelMemoryIsSet && DEFAULT_MODEL_MEMORY_LIMIT.compareTo(maxModelMemoryLimit) > 0) {
                    modelMemoryLimit = maxModelMemoryLimit;
                } else {
                    modelMemoryLimit = DEFAULT_MODEL_MEMORY_LIMIT;
                }
            } else if (maxModelMemoryIsSet && modelMemoryLimit.compareTo(maxModelMemoryLimit) > 0) {
                // Explicit setting higher than limit is an error
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_GREATER_THAN_MAX,
                    modelMemoryLimit, maxModelMemoryLimit));
            }

            return build();
        }

        public DataFrameAnalyticsConfig build() {
            return new DataFrameAnalyticsConfig(id, source, dest, analyses, query, headers, modelMemoryLimit, analysesFields);
        }
    }
}
