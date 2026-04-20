/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.fillRandomBulkFields;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.mutateAbstractBulkByScrollRequest;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.randomResumeInfo;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.resumeInfoOptionalContentHashCode;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.updateByQueryRequestsEqual;

public class UpdateByQueryRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    UpdateByQueryRequestWireSerializingTests.Wrapper> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return BulkByScrollWireSerializingTestUtils.bulkScrollRequestNamedWriteableRegistry();
    }

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices("test-index");
        updateByQueryRequest.getSearchRequest().source().size(between(1, 1000));
        fillRandomBulkFields(updateByQueryRequest);
        if (randomBoolean()) {
            updateByQueryRequest.setPipeline(randomAlphaOfLength(5));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setScript(new Script(ScriptType.STORED, null, randomAlphaOfLength(6), Collections.emptyMap()));
        }
        if (randomBoolean()) {
            updateByQueryRequest.setResumeInfo(randomResumeInfo());
        }
        return new Wrapper(updateByQueryRequest);
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        UpdateByQueryRequest originalRequest = instance.request;
        UpdateByQueryRequest mutatedRequest = copyInstance(instance).request;
        switch (between(0, 2)) {
            case 0 -> mutateAbstractBulkByScrollRequest(originalRequest, mutatedRequest);
            case 1 -> mutatedRequest.setScript(
                randomValueOtherThan(
                    originalRequest.getScript(),
                    () -> new Script(ScriptType.STORED, null, randomAlphaOfLength(11), Collections.emptyMap())
                )
            );
            case 2 -> mutatedRequest.setPipeline(randomValueOtherThan(originalRequest.getPipeline(), () -> randomAlphaOfLength(12)));
            default -> throw new AssertionError();
        }
        return new Wrapper(mutatedRequest);
    }

    static final class Wrapper implements Writeable {
        private final UpdateByQueryRequest request;

        Wrapper(UpdateByQueryRequest request) {
            this.request = request;
        }

        Wrapper(StreamInput streamInput) throws IOException {
            this.request = new UpdateByQueryRequest(streamInput);
        }

        @Override
        public void writeTo(StreamOutput output) throws IOException {
            request.writeTo(output);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            Wrapper wrapper = (Wrapper) other;
            return updateByQueryRequestsEqual(request, wrapper.request);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                request.getSearchRequest(),
                request.getMaxDocs(),
                request.isAbortOnVersionConflict(),
                request.isRefresh(),
                request.getTimeout(),
                request.getWaitForActiveShards(),
                request.getRetryBackoffInitialTime(),
                request.getMaxRetries(),
                request.getRequestsPerSecond(),
                request.getSlices(),
                request.getShouldStoreResult(),
                request.isEligibleForRelocationOnShutdown(),
                resumeInfoOptionalContentHashCode(request.getResumeInfo()),
                Arrays.hashCode(request.getSourceIndicesForDescription()),
                request.getScript(),
                request.getPipeline()
            );
        }
    }
}
