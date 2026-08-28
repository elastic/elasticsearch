/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class DocValuesUpdateRequestTests extends ESTestCase {

    public void testRoundTripThroughDocWriteRequestWireTag() throws Exception {
        DocValuesUpdateRequest request = new DocValuesUpdateRequest(
            "my-index",
            "doc-1",
            List.of(
                new Translog.DocValuesUpdate.NumericFieldUpdate("count", randomLong()),
                new Translog.DocValuesUpdate.BinaryFieldUpdate("status", new BytesRef(randomAlphaOfLengthBetween(1, 20)))
            )
        ).routing(randomBoolean() ? "r" : null)
            .documentVersion(randomNonNegativeLong())
            .operationSeqNo(randomNonNegativeLong(), randomNonNegativeLong())
            .setIfSeqNo(randomNonNegativeLong())
            .setIfPrimaryTerm(randomNonNegativeLong());

        ShardId shardId = new ShardId("my-index", "uuid", 0);

        // full form (as carried in a BulkRequest): shard id is serialized, read back with a null shard id
        BytesStreamOutput fullOut = new BytesStreamOutput();
        DocWriteRequest.writeDocumentRequest(fullOut, request);
        assertRoundTrips(request, DocWriteRequest.readDocumentRequest(null, fullOut.bytes().streamInput()));

        // thin form (as carried in a BulkShardRequest to replicas): shard id provided out of band
        BytesStreamOutput thinOut = new BytesStreamOutput();
        DocWriteRequest.writeDocumentRequestThin(thinOut, request);
        assertRoundTrips(request, DocWriteRequest.readDocumentRequest(shardId, thinOut.bytes().streamInput()));
    }

    public void testValidateRejectsEmptyUpdates() {
        DocValuesUpdateRequest request = new DocValuesUpdateRequest("my-index", "doc-1", List.of());
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("no doc values updates provided"));
    }

    public void testValidateRejectsMissingId() {
        DocValuesUpdateRequest request = new DocValuesUpdateRequest(
            "my-index",
            null,
            List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("count", randomLong()))
        );
        ActionRequestValidationException e = request.validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("id is missing"));
    }

    private static void assertRoundTrips(DocValuesUpdateRequest request, DocWriteRequest<?> read) {
        assertThat(read, instanceOf(DocValuesUpdateRequest.class));
        DocValuesUpdateRequest roundTripped = (DocValuesUpdateRequest) read;
        assertThat(roundTripped.id(), equalTo(request.id()));
        assertThat(roundTripped.routing(), equalTo(request.routing()));
        assertThat(roundTripped.documentVersion(), equalTo(request.documentVersion()));
        assertThat(roundTripped.operationSeqNo(), equalTo(request.operationSeqNo()));
        assertThat(roundTripped.operationPrimaryTerm(), equalTo(request.operationPrimaryTerm()));
        assertThat(roundTripped.ifSeqNo(), equalTo(request.ifSeqNo()));
        assertThat(roundTripped.ifPrimaryTerm(), equalTo(request.ifPrimaryTerm()));
        assertThat(roundTripped.updates(), equalTo(request.updates()));
        assertThat(roundTripped.opType(), equalTo(DocWriteRequest.OpType.INDEX));
    }
}
