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

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class BulkItemRequest implements Streamable {

    private int id;
    private DocWriteRequest request;
    private volatile BulkItemResponse primaryResponse;

    BulkItemRequest() {

    }

    protected BulkItemRequest(int id, DocWriteRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public DocWriteRequest request() {
        return request;
    }

    public String index() {
        assert request.indices().length == 1;
        return request.indices()[0];
    }

    // NOTE: protected for testing only
    protected BulkItemResponse getPrimaryResponse() {
        return primaryResponse;
    }

    // NOTE: protected for testing only
    protected void setPrimaryResponse(BulkItemResponse primaryResponse) {
        this.primaryResponse = primaryResponse;
    }

    public static BulkItemRequest readBulkItem(StreamInput in) throws IOException {
        BulkItemRequest item = new BulkItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        request = DocWriteRequest.readDocumentRequest(in);
        if (in.readBoolean()) {
            primaryResponse = BulkItemResponse.readBulkItem(in);
        }
        if (in.getVersion().before(Version.V_6_0_0_alpha1_UNRELEASED)) { // TODO remove once backported
            boolean ignoreOnReplica = in.readBoolean();
            if (ignoreOnReplica == false && primaryResponse != null) {
                assert primaryResponse.isFailed() == false : "expected no failure on the primary response";
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        if (out.getVersion().before(Version.V_6_0_0_alpha1_UNRELEASED)) { // TODO remove once backported
            // old nodes expect updated version and version type on the request
            if (primaryResponse != null) {
                request.version(primaryResponse.getVersion());
                request.versionType(request.versionType().versionTypeForReplicationAndRecovery());
                DocWriteRequest.writeDocumentRequest(out, request);
            } else {
                DocWriteRequest.writeDocumentRequest(out, request);
            }
        } else {
            DocWriteRequest.writeDocumentRequest(out, request);
        }
        out.writeOptionalStreamable(primaryResponse);
        if (out.getVersion().before(Version.V_6_0_0_alpha1_UNRELEASED)) { // TODO remove once backported
            if (primaryResponse != null) {
                out.writeBoolean(primaryResponse.isFailed()
                        || primaryResponse.getResponse().getResult() == DocWriteResponse.Result.NOOP);
            } else {
                out.writeBoolean(false);
            }
        }
    }
}
