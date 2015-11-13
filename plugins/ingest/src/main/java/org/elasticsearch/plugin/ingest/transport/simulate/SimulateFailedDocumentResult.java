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
package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SimulateFailedDocumentResult extends SimulateDocumentResult {
    public static final int STREAM_ID = 2;

    private Throwable failure;

    public SimulateFailedDocumentResult() {

    }

    public SimulateFailedDocumentResult(Throwable failure) {
        this.failure = failure;
    }

    @Override
    public int getStreamId() {
        return STREAM_ID;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int streamId = in.readVInt();
        if (streamId != STREAM_ID) {
            throw new IOException("stream_id [" + streamId + "] does not match " + getClass().getName() + " [stream_id=" + STREAM_ID + "]");
        }
        this.failure = in.readThrowable();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(STREAM_ID);
        out.writeThrowable(failure);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        ElasticsearchException.renderThrowable(builder, params, failure);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimulateFailedDocumentResult that = (SimulateFailedDocumentResult) o;

        return Objects.equals((failure == null) ? null : failure.getClass(),
                (that.failure == null) ? null : that.failure.getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(failure);
    }
}
