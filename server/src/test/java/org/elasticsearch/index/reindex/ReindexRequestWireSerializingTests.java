/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.fillRandomBulkFields;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.mutateAbstractBulkByScrollRequest;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.randomRemoteInfo;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.randomResumeInfo;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.reindexRequestsEqual;
import static org.elasticsearch.index.reindex.BulkByScrollWireSerializingTestUtils.resumeInfoOptionalContentHashCode;

public class ReindexRequestWireSerializingTests extends AbstractWireSerializingTestCase<ReindexRequestWireSerializingTests.Wrapper> {

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
        return new Wrapper(newRandomReindexWireInstance());
    }

    /**
     * Random {@link ReindexRequest}.
     */
    public static ReindexRequest newRandomReindexWireInstance() {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("source");
        reindexRequest.setDestIndex("destination");
        reindexRequest.getDestination().version(ESTestCase.randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, 12L, 1L, 123124L));
        reindexRequest.getSearchRequest().source().size(ESTestCase.between(1, 1000));
        fillRandomBulkFields(reindexRequest);
        if (ESTestCase.randomBoolean()) {
            reindexRequest.setScript(new Script(ScriptType.STORED, null, ESTestCase.randomAlphaOfLength(6), Collections.emptyMap()));
        }
        if (ESTestCase.randomBoolean()) {
            reindexRequest.setRemoteInfo(randomRemoteInfo());
        }
        if (ESTestCase.randomBoolean()) {
            reindexRequest.setResumeInfo(randomResumeInfo());
        }
        return reindexRequest;
    }

    /**
     * When reading a {@link ReindexRequest} from a node version that predates {@code sourceIndicesForDescription},
     * the field is absent on the wire and must deserialize as {@code null}.
     */
    public void testReindexRequestSourceIndicesForDescriptionBwc() throws IOException {
        ReindexRequest reindex = new ReindexRequest();
        reindex.getSearchRequest().indices(Strings.EMPTY_ARRAY);
        reindex.setSourceIndicesForDescription(new String[] { "source_idx" });
        reindex.setDestIndex("dest");
        reindex.getSearchRequest().source().size(100);

        TransportVersion sourceIndicesForDescriptionVersion = TransportVersion.fromName("bulk_by_scroll_source_indices_for_description");
        TransportVersion versionBeforeFeature = TransportVersionUtils.getPreviousVersion(sourceIndicesForDescriptionVersion);
        assumeTrue(
            "minimumCompatible must not support sourceIndicesForDescription for this BWC test",
            versionBeforeFeature.supports(sourceIndicesForDescriptionVersion) == false
        );

        Wrapper tripped = copyInstance(new Wrapper(reindex), versionBeforeFeature);
        assertNull(
            "sourceIndicesForDescription should be null when reading from version before the feature",
            tripped.request.getSourceIndicesForDescription()
        );
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        ReindexRequest originalRequest = instance.request;
        ReindexRequest mutatedRequest = copyInstance(instance).request;
        mutateReindexRequest(originalRequest, mutatedRequest);
        return new Wrapper(mutatedRequest);
    }

    /**
     * Mutates {@code mutatedRequest} (a copy of {@code originalRequest}) along exactly one logical field: bulk-by-scroll fields,
     * destination index, destination version, remote info, or script.
     */
    public static void mutateReindexRequest(ReindexRequest originalRequest, ReindexRequest mutatedRequest) {
        switch (between(0, 4)) {
            case 0 -> mutateAbstractBulkByScrollRequest(originalRequest, mutatedRequest);
            case 1 -> mutatedRequest.getDestination()
                .index(ESTestCase.randomValueOtherThan(originalRequest.getDestination().index(), () -> ESTestCase.randomAlphaOfLength(14)));
            case 2 -> mutatedRequest.getDestination()
                .version(
                    ESTestCase.randomValueOtherThan(
                        originalRequest.getDestination().version(),
                        () -> ESTestCase.randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, 99L)
                    )
                );
            case 3 -> mutatedRequest.setRemoteInfo(
                ESTestCase.randomValueOtherThan(
                    originalRequest.getRemoteInfo(),
                    () -> ESTestCase.randomBoolean() ? randomRemoteInfo() : null
                )
            );
            case 4 -> mutatedRequest.setScript(
                ESTestCase.randomValueOtherThan(
                    originalRequest.getScript(),
                    () -> new Script(ScriptType.STORED, null, ESTestCase.randomAlphaOfLength(11), Collections.emptyMap())
                )
            );
            default -> throw new AssertionError();
        }
        if (mutatedRequest.getMaxDocs() != AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES
            && mutatedRequest.getMaxDocs() < mutatedRequest.getSlices()) {
            mutatedRequest.setMaxDocs(mutatedRequest.getSlices());
        }
    }

    static final class Wrapper implements Writeable {
        private final ReindexRequest request;

        Wrapper(ReindexRequest request) {
            this.request = request;
        }

        Wrapper(StreamInput streamInput) throws IOException {
            this.request = new ReindexRequest(streamInput);
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
            return reindexRequestsEqual(request, wrapper.request);
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
                request.getDestination().index(),
                request.getDestination().version(),
                request.getRemoteInfo(),
                request.getScript()
            );
        }
    }
}
