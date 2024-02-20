/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetVirtualBatchedCompoundCommitChunkResponse extends ActionResponse {

    private final ReleasableBytesReference data;

    public GetVirtualBatchedCompoundCommitChunkResponse(ReleasableBytesReference data) {
        this.data = data.retain();
    }

    public GetVirtualBatchedCompoundCommitChunkResponse(StreamInput in) throws IOException {
        super(in);
        data = in.readReleasableBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(data);
    }

    public ReleasableBytesReference getData() {
        return data;
    }

    @Override
    public void incRef() {
        data.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return data.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return data.decRef();
    }

    @Override
    public boolean hasReferences() {
        return data.hasReferences();
    }
}
