/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DataFrameAnalyticsConfig implements ToXContentObject, Writeable {

    public static final String TYPE = "data_frame_analytics_config";

    public static final ParseField ID = new ParseField("id");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DEST = new ParseField("dest");
    public static final ParseField ANALYSES = new ParseField("analyses");
    public static final ParseField CONFIG_TYPE = new ParseField("config_type");

    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);

    public static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(TYPE, ignoreUnknownFields, Builder::new);

        parser.declareString((c, s) -> {}, CONFIG_TYPE);
        parser.declareString(Builder::setId, ID);
        parser.declareString(Builder::setSource, SOURCE);
        parser.declareString(Builder::setDest, DEST);
        parser.declareObjectArray(Builder::setAnalyses, DataFrameAnalysisConfig.parser(), ANALYSES);
        return parser;
    }

    private final String id;
    private final String source;
    private final String dest;
    private final List<DataFrameAnalysisConfig> analyses;

    public DataFrameAnalyticsConfig(String id, String source, String dest, List<DataFrameAnalysisConfig> analyses) {
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
    }

    public DataFrameAnalyticsConfig(StreamInput in) throws IOException {
        id = in.readString();
        source = in.readString();
        dest = in.readString();
        analyses = in.readList(DataFrameAnalysisConfig::new);
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
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(source);
        out.writeString(dest);
        out.writeList(analyses);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsConfig other = (DataFrameAnalyticsConfig) o;
        return Objects.equals(id, other.id)
            && Objects.equals(source, other.source)
            && Objects.equals(dest, other.dest)
            && Objects.equals(analyses, other.analyses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, analyses);
    }

    public static String documentId(String id) {
        return TYPE + "-" + id;
    }

    public static class Builder {

        private String id;
        private String source;
        private String dest;
        private List<DataFrameAnalysisConfig> analyses;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, ID);
        }

        public void setSource(String source) {
            this.source = ExceptionsHelper.requireNonNull(source, SOURCE);
        }

        public void setDest(String dest) {
            this.dest = ExceptionsHelper.requireNonNull(dest, DEST);
        }

        public void setAnalyses(List<DataFrameAnalysisConfig> analyses) {
            this.analyses = ExceptionsHelper.requireNonNull(analyses, ANALYSES);
        }

        public DataFrameAnalyticsConfig build() {
            return new DataFrameAnalyticsConfig(id, source, dest, analyses);
        }
    }
}
