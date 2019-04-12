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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING;
import static org.elasticsearch.common.xcontent.ObjectParser.ValueType.VALUE;

public class DataFrameAnalyticsConfig implements ToXContentObject {

    private static final String TYPE = "data_frame_analytics_config";

    private static final ParseField ID = new ParseField("id");
    private static final ParseField SOURCE = new ParseField("source");
    private static final ParseField DEST = new ParseField("dest");
    private static final ParseField ANALYSES = new ParseField("analyses");
    private static final ParseField CONFIG_TYPE = new ParseField("config_type");
    private static final ParseField ANALYSES_FIELDS = new ParseField("analyses_fields");
    private static final ParseField MODEL_MEMORY_LIMIT = new ParseField("model_memory_limit");

    public static DataFrameAnalyticsConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    private static ObjectParser<Builder, Void> PARSER = new ObjectParser<>(TYPE, true, Builder::new);

    static {
        PARSER.declareString((c, s) -> {}, CONFIG_TYPE);
        PARSER.declareString(Builder::setId, ID);
        PARSER.declareObject(Builder::setSource, (p, c) -> DataFrameAnalyticsSource.fromXContent(p), SOURCE);
        PARSER.declareObject(Builder::setDest, (p, c) -> DataFrameAnalyticsDest.fromXContent(p), DEST);
        PARSER.declareObjectArray(Builder::setAnalyses, DataFrameAnalysisConfig.parser(), ANALYSES);
        PARSER.declareField(Builder::setAnalysesFields,
            (p, c) -> FetchSourceContext.fromXContent(p),
            ANALYSES_FIELDS,
            OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareField(Builder::setModelMemoryLimit,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MODEL_MEMORY_LIMIT.getPreferredName()), MODEL_MEMORY_LIMIT, VALUE);
    }

    private final String id;
    private final DataFrameAnalyticsSource source;
    private final DataFrameAnalyticsDest dest;
    private final List<DataFrameAnalysisConfig> analyses;
    private final FetchSourceContext analysesFields;
    private final ByteSizeValue modelMemoryLimit;

    public DataFrameAnalyticsConfig(String id, DataFrameAnalyticsSource source, DataFrameAnalyticsDest dest,
                                    List<DataFrameAnalysisConfig> analyses, ByteSizeValue modelMemoryLimit,
                                    FetchSourceContext analysesFields) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.dest = Objects.requireNonNull(dest);
        this.analyses = Objects.requireNonNull(analyses);
        if (analyses.isEmpty()) {
            throw new ElasticsearchParseException("One or more analyses are required");
        }
        // TODO Add support for multiple analyses
        if (analyses.size() > 1) {
            throw new UnsupportedOperationException("Does not yet support multiple analyses");
        }
        this.analysesFields = analysesFields;
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

    public List<DataFrameAnalysisConfig> getAnalyses() {
        return analyses;
    }

    public FetchSourceContext getAnalysesFields() {
        return analysesFields;
    }

    public ByteSizeValue getModelMemoryLimit() { return modelMemoryLimit; }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(SOURCE.getPreferredName(), source);
        builder.field(DEST.getPreferredName(), dest);
        builder.field(ANALYSES.getPreferredName(), analyses);
        if (analysesFields != null) {
            builder.field(ANALYSES_FIELDS.getPreferredName(), analysesFields);
        }
        builder.field(MODEL_MEMORY_LIMIT.getPreferredName(), getModelMemoryLimit().getStringRep());
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
            && Objects.equals(analyses, other.analyses)
            && Objects.equals(modelMemoryLimit, other.modelMemoryLimit)
            && Objects.equals(analysesFields, other.analysesFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, analyses, getModelMemoryLimit(), analysesFields);
    }

    public static String documentId(String id) {
        return TYPE + "-" + id;
    }

    public static class Builder {

        private String id;
        private DataFrameAnalyticsSource source;
        private DataFrameAnalyticsDest dest;
        private List<DataFrameAnalysisConfig> analyses;
        private FetchSourceContext analysesFields;
        private ByteSizeValue modelMemoryLimit;

        public Builder() {}

        public Builder(String id) {
            setId(id);
        }

        public Builder(DataFrameAnalyticsConfig config) {
            this.id = config.id;
            this.source = new DataFrameAnalyticsSource(config.source);
            this.dest = new DataFrameAnalyticsDest(config.dest);
            this.analyses = new ArrayList<>(config.analyses);
            this.modelMemoryLimit = config.modelMemoryLimit;
            if (config.analysesFields != null) {
                this.analysesFields = new FetchSourceContext(true, config.analysesFields.includes(), config.analysesFields.excludes());
            }
        }

        public String getId() {
            return id;
        }

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

        public Builder setAnalyses(List<DataFrameAnalysisConfig> analyses) {
            this.analyses = Objects.requireNonNull(analyses);
            return this;
        }

        public Builder setAnalysesFields(FetchSourceContext fields) {
            this.analysesFields = fields;
            return this;
        }

        public Builder setModelMemoryLimit(ByteSizeValue modelMemoryLimit) {
            this.modelMemoryLimit = modelMemoryLimit;
            return this;
        }

        public DataFrameAnalyticsConfig build() {
            return new DataFrameAnalyticsConfig(id, source, dest, analyses, modelMemoryLimit, analysesFields);
        }
    }
}
