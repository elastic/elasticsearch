/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING;
import static org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE;
import static org.elasticsearch.xpack.core.ClientHelper.assertNoAuthorizationHeader;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.EXCLUDE_GENERATED;

public class DataFrameAnalyticsConfig implements ToXContentObject, Writeable {

    public static final String BLANK_ID = "blank_data_frame_id";
    public static final String BLANK_DEST_INDEX = "blank_dest_index";

    public static final String TYPE = "data_frame_analytics_config";

    public static final ByteSizeValue DEFAULT_MODEL_MEMORY_LIMIT = ByteSizeValue.ofGb(1);
    public static final ByteSizeValue MIN_MODEL_MEMORY_LIMIT = ByteSizeValue.ofKb(1);
    /**
     * This includes the overhead of thread stacks and data structures that the program might use that
     * are not instrumented.  But it does NOT include the memory used by loading the executable code.
     */
    public static final ByteSizeValue PROCESS_MEMORY_OVERHEAD = ByteSizeValue.ofMb(5);

    public static final ParseField ID = new ParseField("id");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DEST = new ParseField("dest");
    public static final ParseField ANALYSIS = new ParseField("analysis");
    public static final ParseField CONFIG_TYPE = new ParseField("config_type");
    public static final ParseField ANALYZED_FIELDS = new ParseField("analyzed_fields");
    public static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    public static final ParseField HEADERS = new ParseField("headers");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField ALLOW_LAZY_START = new ParseField("allow_lazy_start");
    public static final ParseField MAX_NUM_THREADS = new ParseField("max_num_threads");
    public static final ParseField META = new ParseField("_meta");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(TYPE, ignoreUnknownFields, Builder::new);

        parser.declareString((c, s) -> {}, CONFIG_TYPE);
        parser.declareString(Builder::setId, ID);
        parser.declareString(Builder::setDescription, DESCRIPTION);
        parser.declareObject(Builder::setSource, DataFrameAnalyticsSource.createParser(ignoreUnknownFields), SOURCE);
        parser.declareObject(Builder::setDest, DataFrameAnalyticsDest.createParser(ignoreUnknownFields), DEST);
        parser.declareObject(Builder::setAnalysis, (p, c) -> parseAnalysis(p, ignoreUnknownFields), ANALYSIS);
        parser.declareField(
            Builder::setAnalyzedFields,
            (p, c) -> FetchSourceContext.fromXContent(p),
            ANALYZED_FIELDS,
            OBJECT_ARRAY_BOOLEAN_OR_STRING
        );
        parser.declareField(
            Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()),
            MODEL_MEMORY_LIMIT,
            VALUE
        );
        parser.declareBoolean(Builder::setAllowLazyStart, ALLOW_LAZY_START);
        parser.declareInt(Builder::setMaxNumThreads, MAX_NUM_THREADS);
        parser.declareObject(Builder::setMeta, (p, c) -> p.mapOrdered(), META);
        if (ignoreUnknownFields) {
            // Headers are not parsed by the strict (config) parser, so headers supplied in the _body_ of a REST request will be rejected.
            // (For config, headers are explicitly transferred from the auth headers by code in the put data frame actions.)
            parser.declareObject(Builder::setHeaders, (p, c) -> p.mapStrings(), HEADERS);
            // Creation time is set automatically during PUT, so create_time supplied in the _body_ of a REST request will be rejected.
            parser.declareField(
                Builder::setCreateTime,
                p -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
                CREATE_TIME,
                ObjectParser.ValueType.VALUE
            );
            // Version is set automatically during PUT, so version supplied in the _body_ of a REST request will be rejected.
            parser.declareString(Builder::setVersion, MlConfigVersion::fromString, VERSION);
        }
        return parser;
    }

    private static DataFrameAnalysis parseAnalysis(XContentParser parser, boolean ignoreUnknownFields) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        DataFrameAnalysis analysis = parser.namedObject(DataFrameAnalysis.class, parser.currentName(), ignoreUnknownFields);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return analysis;
    }

    private final String id;
    private final String description;
    private final DataFrameAnalyticsSource source;
    private final DataFrameAnalyticsDest dest;
    private final DataFrameAnalysis analysis;
    private final FetchSourceContext analyzedFields;
    /**
     * This may be null up to the point of persistence, as the relationship with <code>xpack.ml.max_model_memory_limit</code>
     * depends on whether the user explicitly set the value or if the default was requested.  <code>null</code> indicates
     * the default was requested, which in turn means a default higher than the maximum is silently capped.
     * A non-<code>null</code> value higher than <code>xpack.ml.max_model_memory_limit</code> will cause a
     * validation error even if it is equal to the default value.  This behaviour matches what is done in
     * {@link org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits}.
     */
    private final ByteSizeValue modelMemoryLimit;
    private final Map<String, String> headers;
    private final Instant createTime;
    private final MlConfigVersion version;
    private final boolean allowLazyStart;
    private final int maxNumThreads;
    private final Map<String, Object> meta;

    private DataFrameAnalyticsConfig(
        String id,
        String description,
        DataFrameAnalyticsSource source,
        DataFrameAnalyticsDest dest,
        DataFrameAnalysis analysis,
        Map<String, String> headers,
        ByteSizeValue modelMemoryLimit,
        FetchSourceContext analyzedFields,
        Instant createTime,
        MlConfigVersion version,
        boolean allowLazyStart,
        Integer maxNumThreads,
        Map<String, Object> meta
    ) {
        this.id = ExceptionsHelper.requireNonNull(id, ID);
        this.description = description;
        this.source = ExceptionsHelper.requireNonNull(source, SOURCE);
        this.dest = ExceptionsHelper.requireNonNull(dest, DEST);
        this.analysis = ExceptionsHelper.requireNonNull(analysis, ANALYSIS);
        this.analyzedFields = analyzedFields;
        this.modelMemoryLimit = modelMemoryLimit;
        this.headers = Collections.unmodifiableMap(headers);
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());
        this.version = version;
        this.allowLazyStart = allowLazyStart;

        if (maxNumThreads != null && maxNumThreads < 1) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive integer", MAX_NUM_THREADS.getPreferredName());
        }
        this.maxNumThreads = maxNumThreads == null ? 1 : maxNumThreads;
        this.meta = meta == null ? null : Collections.unmodifiableMap(meta);
    }

    public DataFrameAnalyticsConfig(StreamInput in) throws IOException {
        this.id = in.readString();
        this.description = in.readOptionalString();
        this.source = new DataFrameAnalyticsSource(in);
        this.dest = new DataFrameAnalyticsDest(in);
        this.analysis = in.readNamedWriteable(DataFrameAnalysis.class);
        this.analyzedFields = in.readOptionalWriteable(FetchSourceContext::readFrom);
        this.modelMemoryLimit = in.readOptionalWriteable(ByteSizeValue::readFrom);
        this.headers = in.readImmutableMap(StreamInput::readString);
        this.createTime = in.readOptionalInstant();
        this.version = in.readBoolean() ? MlConfigVersion.readVersion(in) : null;
        this.allowLazyStart = in.readBoolean();
        this.maxNumThreads = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            Map<String, Object> readMeta = in.readMap();
            this.meta = readMeta == null ? null : Collections.unmodifiableMap(readMeta);
        } else {
            this.meta = null;
        }
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public DataFrameAnalyticsSource getSource() {
        return source;
    }

    public DataFrameAnalyticsDest getDest() {
        return dest;
    }

    public DataFrameAnalysis getAnalysis() {
        return analysis;
    }

    public FetchSourceContext getAnalyzedFields() {
        return analyzedFields;
    }

    public ByteSizeValue getModelMemoryLimit() {
        return modelMemoryLimit != null ? modelMemoryLimit : DEFAULT_MODEL_MEMORY_LIMIT;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public MlConfigVersion getVersion() {
        return version;
    }

    public boolean isAllowLazyStart() {
        return allowLazyStart;
    }

    public Integer getMaxNumThreads() {
        return maxNumThreads;
    }

    public Map<String, Object> getMeta() {
        return meta;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final boolean forInternalStorage = params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false);
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        if (params.paramAsBoolean(EXCLUDE_GENERATED, false) == false) {
            if (createTime != null) {
                builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
            }
            if (version != null) {
                builder.field(VERSION.getPreferredName(), version);
            }
            if (headers.isEmpty() == false) {
                if (forInternalStorage) {
                    assertNoAuthorizationHeader(headers);
                    builder.field(HEADERS.getPreferredName(), headers);
                } else {
                    XContentUtils.addAuthorizationInfo(builder, headers);
                }
            }
            if (forInternalStorage) {
                builder.field(CONFIG_TYPE.getPreferredName(), TYPE);
            }
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(SOURCE.getPreferredName(), source);
        builder.field(DEST.getPreferredName(), dest);
        builder.startObject(ANALYSIS.getPreferredName());
        builder.field(
            analysis.getWriteableName(),
            analysis,
            new MapParams(Collections.singletonMap(VERSION.getPreferredName(), version == null ? null : version.toString()))
        );
        builder.endObject();
        if (analyzedFields != null) {
            builder.field(ANALYZED_FIELDS.getPreferredName(), analyzedFields);
        }
        builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), getModelMemoryLimit().getStringRep());
        builder.field(ALLOW_LAZY_START.getPreferredName(), allowLazyStart);
        builder.field(MAX_NUM_THREADS.getPreferredName(), maxNumThreads);
        if (meta != null) {
            builder.field(META.getPreferredName(), meta);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeOptionalString(description);
        source.writeTo(out);
        dest.writeTo(out);
        out.writeNamedWriteable(analysis);
        out.writeOptionalWriteable(analyzedFields);
        out.writeOptionalWriteable(modelMemoryLimit);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalInstant(createTime);
        if (version != null) {
            out.writeBoolean(true);
            MlConfigVersion.writeVersion(version, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(allowLazyStart);
        out.writeVInt(maxNumThreads);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeGenericMap(meta);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsConfig other = (DataFrameAnalyticsConfig) o;
        return Objects.equals(id, other.id)
            && Objects.equals(description, other.description)
            && Objects.equals(source, other.source)
            && Objects.equals(dest, other.dest)
            && Objects.equals(analysis, other.analysis)
            && Objects.equals(headers, other.headers)
            && Objects.equals(getModelMemoryLimit(), other.getModelMemoryLimit())
            && Objects.equals(analyzedFields, other.analyzedFields)
            && Objects.equals(createTime, other.createTime)
            && Objects.equals(version, other.version)
            && Objects.equals(allowLazyStart, other.allowLazyStart)
            && maxNumThreads == other.maxNumThreads
            && Objects.equals(meta, other.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            description,
            source,
            dest,
            analysis,
            headers,
            getModelMemoryLimit(),
            analyzedFields,
            createTime,
            version,
            allowLazyStart,
            maxNumThreads,
            meta
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static String documentId(String id) {
        return TYPE + "-" + id;
    }

    /**
     * Returns the job id from the doc id. Returns {@code null} if the doc id is invalid.
     */
    @Nullable
    public static String extractJobIdFromDocId(String docId) {
        String jobId = docId.replaceAll("^" + TYPE + "-", "");
        return jobId.equals(docId) ? null : jobId;
    }

    public static class Builder {

        private String id;
        private String description;
        private DataFrameAnalyticsSource source;
        private DataFrameAnalyticsDest dest;
        private DataFrameAnalysis analysis;
        private FetchSourceContext analyzedFields;
        private ByteSizeValue modelMemoryLimit;
        private ByteSizeValue maxModelMemoryLimit;
        private Map<String, String> headers = Collections.emptyMap();
        private Instant createTime;
        private MlConfigVersion version;
        private boolean allowLazyStart;
        private Integer maxNumThreads;
        private Map<String, Object> meta;

        public Builder() {}

        public Builder(DataFrameAnalyticsConfig config) {
            this(config, null);
        }

        public Builder(DataFrameAnalyticsConfig config, ByteSizeValue maxModelMemoryLimit) {
            this.id = config.id;
            this.description = config.description;
            this.source = new DataFrameAnalyticsSource(config.source);
            this.dest = new DataFrameAnalyticsDest(config.dest);
            this.analysis = config.analysis;
            this.headers = new HashMap<>(config.headers);
            this.modelMemoryLimit = config.modelMemoryLimit;
            this.maxModelMemoryLimit = maxModelMemoryLimit;
            if (config.analyzedFields != null) {
                this.analyzedFields = FetchSourceContext.of(true, config.analyzedFields.includes(), config.analyzedFields.excludes());
            }
            this.createTime = config.createTime;
            this.version = config.version;
            this.allowLazyStart = config.allowLazyStart;
            this.maxNumThreads = config.maxNumThreads;
            this.meta = config.meta;
        }

        public String getId() {
            return id;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setId(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, ID);
            return this;
        }

        public Builder setSource(DataFrameAnalyticsSource source) {
            this.source = ExceptionsHelper.requireNonNull(source, SOURCE);
            return this;
        }

        public Builder setDest(DataFrameAnalyticsDest dest) {
            this.dest = ExceptionsHelper.requireNonNull(dest, DEST);
            return this;
        }

        public Builder setAnalysis(DataFrameAnalysis analysis) {
            this.analysis = ExceptionsHelper.requireNonNull(analysis, ANALYSIS);
            return this;
        }

        public Builder setAnalyzedFields(FetchSourceContext fields) {
            this.analyzedFields = fields;
            return this;
        }

        public Builder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            assertNoAuthorizationHeader(this.headers);
            return this;
        }

        public Builder setModelMemoryLimit(ByteSizeValue modelMemoryLimit) {
            this.modelMemoryLimit = modelMemoryLimit;
            return this;
        }

        public Builder setCreateTime(Instant createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setVersion(MlConfigVersion version) {
            this.version = version;
            return this;
        }

        public Builder setAllowLazyStart(boolean isLazyStart) {
            this.allowLazyStart = isLazyStart;
            return this;
        }

        public Builder setMaxNumThreads(Integer maxNumThreads) {
            this.maxNumThreads = maxNumThreads;
            return this;
        }

        public Builder setMeta(Map<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        /**
         * Builds {@link DataFrameAnalyticsConfig} object.
         */
        public DataFrameAnalyticsConfig build() {
            applyMaxModelMemoryLimit();
            return new DataFrameAnalyticsConfig(
                id,
                description,
                source,
                dest,
                analysis,
                headers,
                modelMemoryLimit,
                analyzedFields,
                createTime,
                version,
                allowLazyStart,
                maxNumThreads,
                meta
            );
        }

        /**
         * Builds {@link DataFrameAnalyticsConfig} object for the purpose of explaining a job that has not been created yet.
         * Some fields (i.e. "id", "dest") may not be present, therefore we overwrite them here to make {@link DataFrameAnalyticsConfig}'s
         * constructor validations happy.
         */
        public DataFrameAnalyticsConfig buildForExplain() {
            return new DataFrameAnalyticsConfig(
                id != null ? id : BLANK_ID,
                description,
                source,
                dest != null ? dest : new DataFrameAnalyticsDest(BLANK_DEST_INDEX, null),
                analysis,
                headers,
                modelMemoryLimit,
                analyzedFields,
                createTime,
                version,
                allowLazyStart,
                maxNumThreads,
                meta
            );
        }

        private void applyMaxModelMemoryLimit() {
            boolean maxModelMemoryIsSet = maxModelMemoryLimit != null && maxModelMemoryLimit.getMb() > 0;

            if (modelMemoryLimit != null) {
                if (modelMemoryLimit.compareTo(MIN_MODEL_MEMORY_LIMIT) < 0) {
                    // Explicit setting lower than minimum is an error
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(
                            Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_TOO_LOW,
                            modelMemoryLimit,
                            MIN_MODEL_MEMORY_LIMIT.getStringRep()
                        )
                    );
                }
                if (maxModelMemoryIsSet && modelMemoryLimit.compareTo(maxModelMemoryLimit) > 0) {
                    // Explicit setting higher than limit is an error
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_GREATER_THAN_MAX, modelMemoryLimit, maxModelMemoryLimit)
                    );
                }
            } else {
                // Default is silently capped if higher than limit
                if (maxModelMemoryIsSet && DEFAULT_MODEL_MEMORY_LIMIT.compareTo(maxModelMemoryLimit) > 0) {
                    modelMemoryLimit = maxModelMemoryLimit;
                }
            }
        }
    }
}
