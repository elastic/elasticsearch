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
package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;

import java.io.IOException;

class SimulateProcessorResult implements Writeable, ToXContentObject {
    private final String processorTag;
    private final WriteableIngestDocument ingestDocument;
    private final Exception failure;

    SimulateProcessorResult(String processorTag, IngestDocument ingestDocument, Exception failure) {
        this.processorTag = processorTag;
        this.ingestDocument = (ingestDocument == null) ? null : new WriteableIngestDocument(ingestDocument);
        this.failure = failure;
    }

    SimulateProcessorResult(String processorTag, IngestDocument ingestDocument) {
        this(processorTag, ingestDocument, null);
    }

    SimulateProcessorResult(String processorTag, Exception failure) {
        this(processorTag, null, failure);
    }

    /**
     * Read from a stream.
     */
    SimulateProcessorResult(StreamInput in) throws IOException {
        this.processorTag = in.readString();
        if (in.readBoolean()) {
            this.ingestDocument = new WriteableIngestDocument(in);
        } else {
            this.ingestDocument = null;
        }
        this.failure = in.readException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorTag);
        if (ingestDocument == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            ingestDocument.writeTo(out);
        }
        out.writeException(failure);
    }

    public IngestDocument getIngestDocument() {
        if (ingestDocument == null) {
            return null;
        }
        return ingestDocument.getIngestDocument();
    }

    public String getProcessorTag() {
        return processorTag;
    }

    public Exception getFailure() {
        return failure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (processorTag != null) {
            builder.field(ConfigurationUtils.TAG_KEY, processorTag);
        }

        if (failure != null && ingestDocument != null) {
            builder.startObject("ignored_error");
            ElasticsearchException.generateFailureXContent(builder, params, failure, true);
            builder.endObject();
        } else if (failure != null) {
            ElasticsearchException.generateFailureXContent(builder, params, failure, true);
        }

        if (ingestDocument != null) {
            ingestDocument.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }
}
