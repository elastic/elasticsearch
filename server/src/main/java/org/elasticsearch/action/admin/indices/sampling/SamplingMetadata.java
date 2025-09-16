/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * An object to store a map from index name to SamplingConfiguration.
 */
public class SamplingMetadata implements Metadata.ProjectCustom {

    private static final String TYPE = "sampling";
    private static final String INDEX_SAMPLING_CONFIG_MAP_FIELD_NAME = "index_to_sampling_config";
    private static final ParseField INDEX_SAMPLING_CONFIG_MAP_PARSE_FIELD = new ParseField(INDEX_SAMPLING_CONFIG_MAP_FIELD_NAME);

    private final Map<String, SamplingConfiguration> indexToSamplingConfigMap;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SamplingMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, true, args -> {
        List<Tuple<String, SamplingConfiguration>> indexToSamplingConfigs = (List<Tuple<String, SamplingConfiguration>>) args[0];
        return new SamplingMetadata(indexToSamplingConfigs.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
    });

    static {
        PARSER.declareNamedObjects(
            constructorArg(),
            (p, c, index) -> Tuple.tuple(index, SamplingConfiguration.fromXContent(p)),
            INDEX_SAMPLING_CONFIG_MAP_PARSE_FIELD
        );
    }

    public SamplingMetadata(Map<String, SamplingConfiguration> indexToSamplingConfigMap) {
        this.indexToSamplingConfigMap = new HashMap<>(indexToSamplingConfigMap);
    }

    public SamplingMetadata(StreamInput in) throws IOException {
        this.indexToSamplingConfigMap = in.readMap(StreamInput::readString, SamplingConfiguration::new);
    }

    public Map<String, SamplingConfiguration> getIndexToSamplingConfigMap() {
        return indexToSamplingConfigMap;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(indexToSamplingConfigMap, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public static SamplingMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(INDEX_SAMPLING_CONFIG_MAP_FIELD_NAME);
            for (Map.Entry<String, SamplingConfiguration> e : indexToSamplingConfigMap.entrySet()) {
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
        SamplingMetadata that = (SamplingMetadata) o;
        return Objects.equals(indexToSamplingConfigMap, that.indexToSamplingConfigMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexToSamplingConfigMap);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_16_0;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.xContentObjectFields(
            INDEX_SAMPLING_CONFIG_MAP_PARSE_FIELD.getPreferredName(),
            indexToSamplingConfigMap
        );
    }

    @Override
    public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom previousState) {
        return null;
    }

    static class SamplingMetadataDiff implements NamedDiff<Metadata.ProjectCustom> {

        final Diff<Map<String, SamplingConfiguration>> indexToSamplingConfigMap;

        SamplingMetadataDiff(SamplingMetadata before, SamplingMetadata after) {
            this.indexToSamplingConfigMap = DiffableUtils.diff(
                before.indexToSamplingConfigMap,
                after.indexToSamplingConfigMap,
                DiffableUtils.getStringKeySerializer()
            );
        }

        SamplingMetadataDiff(StreamInput in) throws IOException {
            indexToSamplingConfigMap = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                SamplingConfiguration::new,
                SamplingConfiguration::readDiffFrom
            );
        }

        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom part) {
            return new SamplingMetadata(indexToSamplingConfigMap.apply(((SamplingMetadata) part).indexToSamplingConfigMap));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexToSamplingConfigMap.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_16_0;
        }
    }

}
