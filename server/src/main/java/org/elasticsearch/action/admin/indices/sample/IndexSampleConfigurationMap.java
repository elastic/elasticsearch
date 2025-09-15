/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sample;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * An object to store a map from index name to SampleConfiguration.
 */
public class IndexSampleConfigurationMap implements Writeable, ToXContentObject {

    private static final String INDEX_SAMPLE_CONFIG_MAP_DOWNLOADER = "index-sample-config-map-downloader";
    private static final String INDEX_SAMPLE_CONFIG_MAP_FIELD = "index_to_sample_config";
    private static final ParseField INDEX_SAMPLE_CONFIG_MAP = new ParseField(INDEX_SAMPLE_CONFIG_MAP_FIELD);

    private final Map<String, SampleConfiguration> indexToSampleConfig;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexSampleConfigurationMap, Void> PARSER = new ConstructingObjectParser<>(
        INDEX_SAMPLE_CONFIG_MAP_DOWNLOADER,
        true,
        args -> {
            List<Tuple<String, SampleConfiguration>> indexToSampleConfigs = (List<Tuple<String, SampleConfiguration>>) args[0];
            return new IndexSampleConfigurationMap(indexToSampleConfigs.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
        }
    );

    static {
        PARSER.declareNamedObjects(
            constructorArg(),
            (p, c, index) -> Tuple.tuple(index, SampleConfiguration.fromXContent(p)),
            INDEX_SAMPLE_CONFIG_MAP
        );
    }

    public IndexSampleConfigurationMap(Map<String, SampleConfiguration> indexToSampleConfig) {
        this.indexToSampleConfig = new HashMap<>(indexToSampleConfig);
    }

    public IndexSampleConfigurationMap(StreamInput in) throws IOException {
        this.indexToSampleConfig = in.readMap(StreamInput::readString, SampleConfiguration::new);
    }

    public Map<String, SampleConfiguration> getIndexToSampleConfig() {
        return indexToSampleConfig;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(indexToSampleConfig, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public static IndexSampleConfigurationMap fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(INDEX_SAMPLE_CONFIG_MAP_FIELD);
            for (Map.Entry<String, SampleConfiguration> e : indexToSampleConfig.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexSampleConfigurationMap that = (IndexSampleConfigurationMap) o;
        return Objects.equals(indexToSampleConfig, that.indexToSampleConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexToSampleConfig);
    }

}
