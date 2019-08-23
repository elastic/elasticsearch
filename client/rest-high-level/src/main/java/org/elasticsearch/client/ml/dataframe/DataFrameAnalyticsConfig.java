/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.client.dataframe.transforms.util.TimeUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class DataFrameAnalyticsConfig implements ToXContentObject {

    public static DataFrameAnalyticsConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static final ParseField ID = new ParseField("id");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField SOURCE = new ParseField("source");
    private static final ParseField DEST = new ParseField("dest");
    private static final ParseField ANALYSIS = new ParseField("analysis");
    private static final ParseField ANALYZED_FIELDS = new ParseField("analyzed_fields");
    private static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");
    private static final ParseField CREATE_TIME = new ParseField("create_time");
    private static final ParseField VERSION = new ParseField("version");

    private static ObjectParser<Builder, Void> PARSER = new ObjectParser<>("data_frame_analytics_config", true, Builder::new);

    static {
        PARSER.declareString(Builder::setId, ID);
        PARSER.declareString(Builder::setDescription, DESCRIPTION);
        PARSER.declareObject(Builder::setSource, (p, c) -> DataFrameAnalyticsSource.fromXContent(p), SOURCE);
        PARSER.declareObject(Builder::setDest, (p, c) -> DataFrameAnalyticsDest.fromXContent(p), DEST);
        PARSER.declareObject(Builder::setAnalysis, (p, c) -> parseAnalysis(p), ANALYSIS);
        PARSER.declareField(Builder::setAnalyzedFields,
            (p, c) -> FetchSourceContext.fromXContent(p),
            ANALYZED_FIELDS,
            ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareField(Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()),
            MODEL_MEMORY_LIMIT,
            ValueType.VALUE);
        PARSER.declareField(Builder::setCreateTime,
            p -> TimeUtil.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()),
            CREATE_TIME,
            ValueType.VALUE);
        PARSER.declareField(Builder::setVersion,
            p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return Version.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            },
            VERSION,
            ValueType.STRING);
    }

    private static DataFrameAnalysis parseAnalysis(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        DataFrameAnalysis analysis = parser.namedObject(DataFrameAnalysis.class, parser.currentName(), true);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return analysis;
    }

    private final String id;
    private final String description;
    private final DataFrameAnalyticsSource source;
    private final DataFrameAnalyticsDest dest;
    private final DataFrameAnalysis analysis;
    private final FetchSourceContext analyzedFields;
    private final ByteSizeValue modelMemoryLimit;
    private final Instant createTime;
    private final Version version;

    private DataFrameAnalyticsConfig(@Nullable String id, @Nullable String description, @Nullable DataFrameAnalyticsSource source,
                                     @Nullable DataFrameAnalyticsDest dest, @Nullable DataFrameAnalysis analysis,
                                     @Nullable FetchSourceContext analyzedFields, @Nullable ByteSizeValue modelMemoryLimit,
                                     @Nullable Instant createTime, @Nullable Version version) {
        this.id = id;
        this.description = description;
        this.source = source;
        this.dest = dest;
        this.analysis = analysis;
        this.analyzedFields = analyzedFields;
        this.modelMemoryLimit = modelMemoryLimit;
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());;
        this.version = version;
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
        return modelMemoryLimit;
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
        if (id != null) {
            builder.field(ID.getPreferredName(), id);
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (source != null) {
            builder.field(SOURCE.getPreferredName(), source);
        }
        if (dest != null) {
            builder.field(DEST.getPreferredName(), dest);
        }
        if (analysis != null) {
            builder
                .startObject(ANALYSIS.getPreferredName())
                .field(analysis.getName(), analysis)
                .endObject();
        }
        if (analyzedFields != null) {
            builder.field(ANALYZED_FIELDS.getPreferredName(), analyzedFields);
        }
        if (modelMemoryLimit != null) {
            builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit.getStringRep());
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
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsConfig other = (DataFrameAnalyticsConfig) o;
        return Objects.equals(id, other.id)
            && Objects.equals(description, other.description)
            && Objects.equals(source, other.source)
            && Objects.equals(dest, other.dest)
            && Objects.equals(analysis, other.analysis)
            && Objects.equals(analyzedFields, other.analyzedFields)
            && Objects.equals(modelMemoryLimit, other.modelMemoryLimit)
            && Objects.equals(createTime, other.createTime)
            && Objects.equals(version, other.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, source, dest, analysis, analyzedFields, modelMemoryLimit, createTime, version);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String id;
        private String description;
        private DataFrameAnalyticsSource source;
        private DataFrameAnalyticsDest dest;
        private DataFrameAnalysis analysis;
        private FetchSourceContext analyzedFields;
        private ByteSizeValue modelMemoryLimit;
        private Instant createTime;
        private Version version;

        private Builder() {}

        public Builder setId(String id) {
            this.id = Objects.requireNonNull(id);
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setSource(DataFrameAnalyticsSource source) {
            this.source = Objects.requireNonNull(source);
            return this;
        }

        public Builder setDest(DataFrameAnalyticsDest dest) {
            this.dest = Objects.requireNonNull(dest);
            return this;
        }

        public Builder setAnalysis(DataFrameAnalysis analysis) {
            this.analysis = Objects.requireNonNull(analysis);
            return this;
        }

        public Builder setAnalyzedFields(FetchSourceContext fields) {
            this.analyzedFields = fields;
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

        public DataFrameAnalyticsConfig build() {
            return new DataFrameAnalyticsConfig(id, description, source, dest, analysis, analyzedFields, modelMemoryLimit, createTime,
                version);
        }
    }
}
