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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING;
import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.VALUE;

public class DataFrameAnalyticsConfig implements ToXContentObject {

    public static DataFrameAnalyticsConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static Builder builder(String id) {
        return new Builder().setId(id);
    }

    private static final ParseField ID = new ParseField("id");
    private static final ParseField SOURCE = new ParseField("source");
    private static final ParseField DEST = new ParseField("dest");
    private static final ParseField ANALYSIS = new ParseField("analysis");
    private static final ParseField ANALYZED_FIELDS = new ParseField("analyzed_fields");
    private static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");

    private static ObjectParser<Builder, Void> PARSER = new ObjectParser<>("data_frame_analytics_config", true, Builder::new);

    static {
        PARSER.declareString(Builder::setId, ID);
        PARSER.declareObject(Builder::setSource, (p, c) -> DataFrameAnalyticsSource.fromXContent(p), SOURCE);
        PARSER.declareObject(Builder::setDest, (p, c) -> DataFrameAnalyticsDest.fromXContent(p), DEST);
        PARSER.declareObject(Builder::setAnalysis, (p, c) -> parseAnalysis(p), ANALYSIS);
        PARSER.declareField(Builder::setAnalyzedFields,
            (p, c) -> FetchSourceContext.fromXContent(p),
            ANALYZED_FIELDS,
            OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareField(Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()), MODEL_MEMORY_LIMIT, VALUE);
    }

    private static DataFrameAnalysis parseAnalysis(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        DataFrameAnalysis analysis = parser.namedObject(DataFrameAnalysis.class, parser.currentName(), true);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return analysis;
    }

    private final String id;
    private final DataFrameAnalyticsSource source;
    private final DataFrameAnalyticsDest dest;
    private final DataFrameAnalysis analysis;
    private final FetchSourceContext analyzedFields;
    private final ByteSizeValue modelMemoryLimit;

    private DataFrameAnalyticsConfig(String id, DataFrameAnalyticsSource source, DataFrameAnalyticsDest dest, DataFrameAnalysis analysis,
                                     @Nullable FetchSourceContext analyzedFields, @Nullable ByteSizeValue modelMemoryLimit) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.dest = Objects.requireNonNull(dest);
        this.analysis = Objects.requireNonNull(analysis);
        this.analyzedFields = analyzedFields;
        this.modelMemoryLimit = modelMemoryLimit;
    }

    public String getId() {
        return id;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(SOURCE.getPreferredName(), source);
        builder.field(DEST.getPreferredName(), dest);
        builder.startObject(ANALYSIS.getPreferredName());
        builder.field(analysis.getName(), analysis);
        builder.endObject();
        if (analyzedFields != null) {
            builder.field(ANALYZED_FIELDS.getPreferredName(), analyzedFields);
        }
        if (modelMemoryLimit != null) {
            builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), modelMemoryLimit.getStringRep());
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
            && Objects.equals(source, other.source)
            && Objects.equals(dest, other.dest)
            && Objects.equals(analysis, other.analysis)
            && Objects.equals(analyzedFields, other.analyzedFields)
            && Objects.equals(modelMemoryLimit, other.modelMemoryLimit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, analysis, analyzedFields, getModelMemoryLimit());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String id;
        private DataFrameAnalyticsSource source;
        private DataFrameAnalyticsDest dest;
        private DataFrameAnalysis analysis;
        private FetchSourceContext analyzedFields;
        private ByteSizeValue modelMemoryLimit;

        private Builder() {}

        public Builder setId(String id) {
            this.id = Objects.requireNonNull(id);
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

        public DataFrameAnalyticsConfig build() {
            return new DataFrameAnalyticsConfig(id, source, dest, analysis, analyzedFields, modelMemoryLimit);
        }
    }
}
