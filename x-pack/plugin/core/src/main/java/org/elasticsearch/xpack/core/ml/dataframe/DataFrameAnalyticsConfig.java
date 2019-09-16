/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING;
import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.VALUE;

public class DataFrameAnalyticsConfig implements ToXContentObject, Writeable {

    public static final String TYPE = "data_frame_analytics_config";

    public static final ByteSizeValue DEFAULT_MODEL_MEMORY_LIMIT = new ByteSizeValue(1, ByteSizeUnit.GB);
    public static final ByteSizeValue MIN_MODEL_MEMORY_LIMIT = new ByteSizeValue(1, ByteSizeUnit.MB);
    public static final ByteSizeValue PROCESS_MEMORY_OVERHEAD = new ByteSizeValue(20, ByteSizeUnit.MB);

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
        parser.declareField(Builder::setAnalyzedFields,
            (p, c) -> FetchSourceContext.fromXContent(p),
            ANALYZED_FIELDS,
            OBJECT_ARRAY_BOOLEAN_OR_STRING);
        parser.declareField(Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()), MODEL_MEMORY_LIMIT, VALUE);
        if (ignoreUnknownFields) {
            // Headers are not parsed by the strict (config) parser, so headers supplied in the _body_ of a REST request will be rejected.
            // (For config, headers are explicitly transferred from the auth headers by code in the put data frame actions.)
            parser.declareObject(Builder::setHeaders, (p, c) -> p.mapStrings(), HEADERS);
            // Creation time is set automatically during PUT, so create_time supplied in the _body_ of a REST request will be rejected.
            parser.declareField(Builder::setCreateTime,
                p -> TimeUtils.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
                CREATE_TIME,
                ObjectParser.ValueType.VALUE);
            // Version is set automatically during PUT, so version supplied in the _body_ of a REST request will be rejected.
            parser.declareField(Builder::setVersion, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return Version.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, VERSION, ObjectParser.ValueType.STRING);
        }
        return parser;
    }

    private static DataFrameAnalysis parseAnalysis(XContentParser parser, boolean ignoreUnknownFields) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        DataFrameAnalysis analysis = parser.namedObject(DataFrameAnalysis.class, parser.currentName(), ignoreUnknownFields);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
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
    private final Version version;

    public DataFrameAnalyticsConfig(String id, String description, DataFrameAnalyticsSource source, DataFrameAnalyticsDest dest,
                                    DataFrameAnalysis analysis, Map<String, String> headers, ByteSizeValue modelMemoryLimit,
                                    FetchSourceContext analyzedFields, Instant createTime, Version version) {
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
    }

    public DataFrameAnalyticsConfig(StreamInput in) throws IOException {
        this.id = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            description = in.readOptionalString();
        } else {
            description = null;
        }
        this.source = new DataFrameAnalyticsSource(in);
        this.dest = new DataFrameAnalyticsDest(in);
        this.analysis = in.readNamedWriteable(DataFrameAnalysis.class);
        this.analyzedFields = in.readOptionalWriteable(FetchSourceContext::new);
        this.modelMemoryLimit = in.readOptionalWriteable(ByteSizeValue::new);
        this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            createTime = in.readOptionalInstant();
            version = in.readBoolean() ? Version.readVersion(in) : null;
        } else {
            createTime = null;
            version = null;
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

    public Version getVersion() {
        return version;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        builder.field(SOURCE.getPreferredName(), source);
        builder.field(DEST.getPreferredName(), dest);

        builder.startObject(ANALYSIS.getPreferredName());
        builder.field(analysis.getWriteableName(), analysis);
        builder.endObject();

        if (params.paramAsBoolean(ToXContentParams.INCLUDE_TYPE, false)) {
            builder.field(CONFIG_TYPE.getPreferredName(), TYPE);
        }
        if (analyzedFields != null) {
            builder.field(ANALYZED_FIELDS.getPreferredName(), analyzedFields);
        }
        builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), getModelMemoryLimit().getStringRep());
        if (headers.isEmpty() == false && params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(HEADERS.getPreferredName(), headers);
        }
        if (createTime != null) {
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        }
        if (version != null) {
            builder.field(VERSION.getPreferredName(), version);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeOptionalString(description);
        }
        source.writeTo(out);
        dest.writeTo(out);
        out.writeNamedWriteable(analysis);
        out.writeOptionalWriteable(analyzedFields);
        out.writeOptionalWriteable(modelMemoryLimit);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalInstant(createTime);
            if (version != null) {
                out.writeBoolean(true);
                Version.writeVersion(version, out);
            } else {
                out.writeBoolean(false);
            }
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
            && Objects.equals(version, other.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, source, dest, analysis, headers, getModelMemoryLimit(), analyzedFields, createTime, version);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static String documentId(String id) {
        return TYPE + "-" + id;
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
        private Version version;

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
                this.analyzedFields = new FetchSourceContext(true, config.analyzedFields.includes(), config.analyzedFields.excludes());
            }
            this.createTime = config.createTime;
            this.version = config.version;
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

        public Builder setVersion(Version version) {
            this.version = version;
            return this;
        }

        /**
         * Builds {@link DataFrameAnalyticsConfig} object.
         */
        public DataFrameAnalyticsConfig build() {
            applyMaxModelMemoryLimit();
            return new DataFrameAnalyticsConfig(id, description, source, dest, analysis, headers, modelMemoryLimit, analyzedFields,
                createTime, version);
        }

        /**
         * Builds {@link DataFrameAnalyticsConfig} object for the purpose of performing memory estimation.
         * Some fields (i.e. "id", "dest") may not be present, therefore we overwrite them here to make {@link DataFrameAnalyticsConfig}'s
         * constructor validations happy.
         */
        public DataFrameAnalyticsConfig buildForMemoryEstimation() {
            return new DataFrameAnalyticsConfig(
                id != null ? id : "dummy",
                description,
                source,
                dest != null ? dest : new DataFrameAnalyticsDest("dummy", null),
                analysis,
                headers,
                modelMemoryLimit,
                analyzedFields,
                createTime,
                version);
        }

        private void applyMaxModelMemoryLimit() {
            boolean maxModelMemoryIsSet = maxModelMemoryLimit != null && maxModelMemoryLimit.getMb() > 0;

            if (modelMemoryLimit != null) {
                if (modelMemoryLimit.compareTo(MIN_MODEL_MEMORY_LIMIT) < 0) {
                    // Explicit setting lower than minimum is an error
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_TOO_LOW, modelMemoryLimit));
                }
                if (maxModelMemoryIsSet && modelMemoryLimit.compareTo(maxModelMemoryLimit) > 0) {
                    // Explicit setting higher than limit is an error
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(
                            Messages.JOB_CONFIG_MODEL_MEMORY_LIMIT_GREATER_THAN_MAX, modelMemoryLimit, maxModelMemoryLimit));
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
