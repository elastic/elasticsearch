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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SimulatePipelineResponse extends ActionResponse implements ToXContent {
    private String pipelineId;
    private List<SimulateDocumentResult> results;

    public SimulatePipelineResponse() {

    }

    public SimulatePipelineResponse(String pipelineId, List<SimulateDocumentResult> responses) {
        this.pipelineId = pipelineId;
        this.results = Collections.unmodifiableList(responses);
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public List<SimulateDocumentResult> getResults() {
        return results;
    }

    public void setResults(List<SimulateDocumentResult> results) {
        this.results = results;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pipelineId);
        out.writeVInt(results.size());
        for (SimulateDocumentResult response : results) {
            out.writeVInt(response.getStreamId());
            response.writeTo(out);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.pipelineId = in.readString();
        int responsesLength = in.readVInt();
        results = new ArrayList<>();
        for (int i = 0; i < responsesLength; i++) {
            SimulateDocumentResult result;
            switch (in.readVInt()) {
                case SimulateSimpleDocumentResult.STREAM_ID:
                    result = new SimulateSimpleDocumentResult();
                    break;
                case SimulateVerboseDocumentResult.STREAM_ID:
                    result = new SimulateVerboseDocumentResult();
                    break;
                case SimulateFailedDocumentResult.STREAM_ID:
                    result = new SimulateFailedDocumentResult();
                    break;
                default:
                    throw new IOException("Cannot read result from stream");
            }
            result.readFrom(in);
            results.add(result);
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.DOCUMENTS);
        for (SimulateDocumentResult response : results) {
            response.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimulatePipelineResponse that = (SimulatePipelineResponse) o;
        return Objects.equals(pipelineId, that.pipelineId) &&
                Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineId, results);
    }

    static final class Fields {
        static final XContentBuilderString DOCUMENTS = new XContentBuilderString("docs");
    }
}
