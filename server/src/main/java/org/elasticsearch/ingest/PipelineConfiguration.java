/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates a pipeline's id and configuration as a blob
 */
public final class PipelineConfiguration extends AbstractDiffable<PipelineConfiguration> implements ToXContentObject {

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("pipeline_config", true, Builder::new);
    static {
        PARSER.declareString(Builder::setId, new ParseField("id"));
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(parser);
            builder.setConfig(BytesReference.bytes(contentBuilder), contentBuilder.contentType());
        }, new ParseField("config"), ObjectParser.ValueType.OBJECT);

    }

    public static ContextParser<Void, PipelineConfiguration> getParser() {
        return (parser, context) -> PARSER.apply(parser, null).build();
    }
    private static class Builder {

        private String id;
        private BytesReference config;
        private XContentType xContentType;

        void setId(String id) {
            this.id = id;
        }

        void setConfig(BytesReference config, XContentType xContentType) {
            this.config = config;
            this.xContentType = xContentType;
        }

        PipelineConfiguration build() {
            return new PipelineConfiguration(id, config, xContentType);
        }
    }

    private final String id;
    // Store config as bytes reference, because the config is only used when the pipeline store reads the cluster state
    // and the way the map of maps config is read requires a deep copy (it removes instead of gets entries to check for unused options)
    // also the get pipeline api just directly returns this to the caller
    private final BytesReference config;
    private final XContentType xContentType;

    public PipelineConfiguration(String id, BytesReference config, XContentType xContentType) {
        this.id = Objects.requireNonNull(id);
        this.config = Objects.requireNonNull(config);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getConfigAsMap() {
        return XContentHelper.convertToMap(config, true, xContentType).v2();
    }

    // pkg-private for tests
    XContentType getXContentType() {
        return xContentType;
    }

    // pkg-private for tests
    BytesReference getConfig() {
        return config;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("config", getConfigAsMap());
        builder.endObject();
        return builder;
    }

    public static PipelineConfiguration readFrom(StreamInput in) throws IOException {
        return new PipelineConfiguration(in.readString(), in.readBytesReference(), in.readEnum(XContentType.class));
    }

    public static Diff<PipelineConfiguration> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(PipelineConfiguration::readFrom, in);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBytesReference(config);
        XContentHelper.writeTo(out, xContentType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PipelineConfiguration that = (PipelineConfiguration) o;

        if (id.equals(that.id) == false) return false;
        return getConfigAsMap().equals(that.getConfigAsMap());

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + getConfigAsMap().hashCode();
        return result;
    }
}
