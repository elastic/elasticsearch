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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;

import java.io.IOException;

public class SimulateProcessorResult implements Writeable, ToXContent {
    private final String processorTag;
    private final WriteableIngestDocument ingestDocument;
    private final Exception failure;

    public SimulateProcessorResult(String processorTag, IngestDocument ingestDocument) {
        this.processorTag = processorTag;
        this.ingestDocument = new WriteableIngestDocument(ingestDocument);
        this.failure = null;
    }

    public SimulateProcessorResult(String processorTag, Exception failure) {
        this.processorTag = processorTag;
        this.failure = failure;
        this.ingestDocument = null;
    }

    /**
     * Read from a stream.
     */
    public SimulateProcessorResult(StreamInput in) throws IOException {
        this.processorTag = in.readString();
        if (in.readBoolean()) {
            this.failure = in.readThrowable();
            this.ingestDocument = null;
        } else {
            this.ingestDocument = new WriteableIngestDocument(in);
            this.failure = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorTag);
        if (failure == null) {
            out.writeBoolean(false);
            ingestDocument.writeTo(out);
        } else {
            out.writeBoolean(true);
            out.writeThrowable(failure);
        }
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
            builder.field(AbstractProcessorFactory.TAG_KEY, processorTag);
        }
        if (failure == null) {
            ingestDocument.toXContent(builder, params);
        } else {
            ElasticsearchException.renderThrowable(builder, params, failure);
        }
        builder.endObject();
        return builder;
    }
}
