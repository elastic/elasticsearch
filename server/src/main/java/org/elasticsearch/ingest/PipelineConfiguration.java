/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates a pipeline's id and configuration as a blob
 */
public final class PipelineConfiguration extends AbstractDiffable<PipelineConfiguration> implements ToXContentObject {

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("pipeline_config", Builder::new);
    static {
        PARSER.declareString(Builder::setId, new ParseField("id"));
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            XContentHelper.copyCurrentStructure(contentBuilder.generator(), parser);
            builder.setConfig(contentBuilder.bytes(), contentBuilder.contentType());
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
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            return new PipelineConfiguration(in.readString(), in.readBytesReference(), XContentType.readFrom(in));
        } else {
            final String id = in.readString();
            final BytesReference config = in.readBytesReference();
            return new PipelineConfiguration(id, config, XContentFactory.xContentType(config));
        }
    }

    public static Diff<PipelineConfiguration> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(PipelineConfiguration::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBytesReference(config);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PipelineConfiguration that = (PipelineConfiguration) o;

        if (!id.equals(that.id)) return false;
        return config.equals(that.config);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + config.hashCode();
        return result;
    }
}
