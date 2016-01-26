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

package org.elasticsearch.ingest.core;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.ingest.processor.ConfigurationPropertyException;

import java.io.IOException;

public class PipelineFactoryError implements Streamable, ToXContent {
    private String reason;
    private String processorType;
    private String processorTag;
    private String processorPropertyName;

    public PipelineFactoryError() {

    }

    public PipelineFactoryError(ConfigurationPropertyException e) {
        this.reason = e.getMessage();
        this.processorType = e.getProcessorType();
        this.processorTag = e.getProcessorTag();
        this.processorPropertyName = e.getPropertyName();
    }

    public PipelineFactoryError(String reason) {
        this.reason = "Constructing Pipeline failed:" + reason;
    }

    public String getReason() {
        return reason;
    }

    public String getProcessorTag() {
        return processorTag;
    }

    public String getProcessorPropertyName() {
        return processorPropertyName;
    }

    public String getProcessorType() {
        return processorType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        reason = in.readString();
        processorType = in.readOptionalString();
        processorTag = in.readOptionalString();
        processorPropertyName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(reason);
        out.writeOptionalString(processorType);
        out.writeOptionalString(processorTag);
        out.writeOptionalString(processorPropertyName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("error");
        builder.field("type", processorType);
        builder.field("tag", processorTag);
        builder.field("reason", reason);
        builder.field("property_name", processorPropertyName);
        builder.endObject();
        return builder;
    }
}
