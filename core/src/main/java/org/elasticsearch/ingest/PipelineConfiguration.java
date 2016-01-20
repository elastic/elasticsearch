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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates a pipeline's id and configuration as a blob
 */
public final class PipelineConfiguration implements Writeable<PipelineConfiguration>, ToXContent {

    private final static PipelineConfiguration PROTOTYPE = new PipelineConfiguration(null, null);

    public static PipelineConfiguration readPipelineConfiguration(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    public static class Builder {

        private final static ObjectParser<Builder, Void> PARSER = new ObjectParser<>("pipeline_config", null);

        static {
            PARSER.declareString(Builder::setId, new ParseField("id"));
            PARSER.declareField((parser, builder, aVoid) -> {
                XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
                XContentHelper.copyCurrentEvent(contentBuilder.generator(), parser);
                builder.setConfig(contentBuilder.bytes());
            }, new ParseField("config"), ObjectParser.ValueType.OBJECT);
        }

        private String id;
        private BytesReference config;

        public Builder(XContentParser parser) throws IOException {
            PARSER.parse(parser, this);
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setConfig(BytesReference config) {
            this.config = config;
        }

        public PipelineConfiguration build() {
            return new PipelineConfiguration(id, config);
        }
    }

    private final String id;
    // Store config as bytes reference, because the config is only used when the pipeline store reads the cluster state
    // and the way the map of maps config is read requires a deep copy (it removes instead of gets entries to check for unused options)
    // also the get pipeline api just directly returns this to the caller
    private final BytesReference config;

    public PipelineConfiguration(String id, BytesReference config) {
        this.id = id;
        this.config = config;
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getConfigAsMap() {
        return XContentHelper.convertToMap(config, true).v2();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("config", getConfigAsMap());
        builder.endObject();
        return builder;
    }

    @Override
    public PipelineConfiguration readFrom(StreamInput in) throws IOException {
        return new PipelineConfiguration(in.readString(), in.readBytesReference());
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBytesReference(config);
    }

}
