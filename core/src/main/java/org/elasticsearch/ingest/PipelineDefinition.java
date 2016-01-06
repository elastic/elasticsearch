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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.core.Pipeline;

import java.io.IOException;

public class PipelineDefinition implements Writeable<PipelineDefinition>, ToXContent {

    private static final PipelineDefinition PROTOTYPE = new PipelineDefinition((String) null, -1, null);

    public static PipelineDefinition readPipelineDefinitionFrom(StreamInput in) throws IOException {
        return PROTOTYPE.readFrom(in);
    }

    private final String id;
    private final long version;
    private final BytesReference source;

    private final Pipeline pipeline;

    PipelineDefinition(Pipeline pipeline, long version, BytesReference source) {
        this.id = pipeline.getId();
        this.version = version;
        this.source = source;
        this.pipeline = pipeline;
    }

    PipelineDefinition(String id, long version, BytesReference source) {
        this.id = id;
        this.version = version;
        this.source = source;
        this.pipeline = null;
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public BytesReference getSource() {
        return source;
    }

    Pipeline getPipeline() {
        return pipeline;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PipelineDefinition holder = (PipelineDefinition) o;
        return source.equals(holder.source);
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    @Override
    public PipelineDefinition readFrom(StreamInput in) throws IOException {
        String id = in.readString();
        long version = in.readLong();
        BytesReference source = in.readBytesReference();
        return new PipelineDefinition(id, version, source);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeBytesReference(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(id);
        XContentHelper.writeRawField("_source", source, builder, params);
        builder.field("_version", version);
        builder.endObject();
        return builder;
    }
}
