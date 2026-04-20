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
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.reindexRequestsEqual;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.resumeInfoOptionalContentHashCode;
import static org.elasticsearch.index.reindex.ReindexRequestWireSerializingTests.mutateReindexRequest;
import static org.elasticsearch.index.reindex.ReindexRequestWireSerializingTests.newRandomReindexWireInstance;

public class ResumeBulkByScrollRequestWireSerializingTests extends AbstractWireSerializingTestCase<
    ResumeBulkByScrollRequestWireSerializingTests.Wrapper> {

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
        return new Wrapper(new ResumeBulkByScrollRequest(newRandomReindexWireInstance()));
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        ResumeBulkByScrollRequest originalRequest = instance.request;
        ResumeBulkByScrollRequest mutatedResumeRequest = copyInstance(instance).request;
        ReindexRequest originalReindex = (ReindexRequest) originalRequest.getDelegate();
        ReindexRequest mutatedReindex = (ReindexRequest) mutatedResumeRequest.getDelegate();
        mutateReindexRequest(originalReindex, mutatedReindex);
        return new Wrapper(new ResumeBulkByScrollRequest(mutatedReindex));
    }

    static final class Wrapper implements Writeable {
        private final ResumeBulkByScrollRequest request;

        Wrapper(ResumeBulkByScrollRequest request) {
            this.request = request;
        }

        Wrapper(StreamInput streamInput) throws IOException {
            this.request = new ResumeBulkByScrollRequest(streamInput, ReindexRequest::new);
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
            return reindexRequestsEqual((ReindexRequest) request.getDelegate(), (ReindexRequest) wrapper.request.getDelegate());
        }

        @Override
        public int hashCode() {
            ReindexRequest reindexDelegate = (ReindexRequest) request.getDelegate();
            return Objects.hash(
                reindexDelegate.getSearchRequest(),
                reindexDelegate.getMaxDocs(),
                reindexDelegate.isAbortOnVersionConflict(),
                reindexDelegate.isRefresh(),
                reindexDelegate.getTimeout(),
                reindexDelegate.getWaitForActiveShards(),
                reindexDelegate.getRetryBackoffInitialTime(),
                reindexDelegate.getMaxRetries(),
                reindexDelegate.getRequestsPerSecond(),
                reindexDelegate.getSlices(),
                reindexDelegate.getShouldStoreResult(),
                reindexDelegate.isEligibleForRelocationOnShutdown(),
                resumeInfoOptionalContentHashCode(reindexDelegate.getResumeInfo()),
                Arrays.hashCode(reindexDelegate.getSourceIndicesForDescription()),
                reindexDelegate.getDestination().index(),
                reindexDelegate.getDestination().version(),
                reindexDelegate.getRemoteInfo(),
                reindexDelegate.getScript()
            );
        }
    }
}
