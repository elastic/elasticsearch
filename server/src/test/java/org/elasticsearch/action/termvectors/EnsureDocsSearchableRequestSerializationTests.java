/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.termvectors.EnsureDocsSearchableAction.EnsureDocsSearchableRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.nullValue;

/**
 * Wire round-trip coverage for {@link EnsureDocsSearchableRequest}, in particular the per-document {@code routings}
 * field added alongside the {@code eds_routing} transport version: it must survive a round-trip on a current node
 * (including {@code null} elements for docs with no routing) and must be dropped to {@code null} when the request is
 * sent to a node that predates {@code eds_routing}.
 */
public class EnsureDocsSearchableRequestSerializationTests extends ESTestCase {

    private static final TransportVersion ROUTING = TransportVersion.fromName("eds_routing");

    public void testRoundTripWithRoutings() throws IOException {
        final String[] docIds = { "1", "2", "3" };
        // include a null element to model a document with no routing
        final String[] routings = { "r1", null, "r3" };
        final EnsureDocsSearchableRequest request = new EnsureDocsSearchableRequest(
            "index",
            0,
            docIds,
            routings,
            SplitShardCountSummary.UNSET
        );

        final EnsureDocsSearchableRequest deserialized = copy(request, TransportVersion.current());
        assertThat(deserialized.docIds(), arrayContaining(docIds));
        assertThat(deserialized.routings(), arrayContaining("r1", null, "r3"));
    }

    public void testRoundTripWithNullRoutings() throws IOException {
        final String[] docIds = { "1", "2" };
        final EnsureDocsSearchableRequest request = new EnsureDocsSearchableRequest("index", 0, docIds, null, SplitShardCountSummary.UNSET);

        final EnsureDocsSearchableRequest deserialized = copy(request, TransportVersion.current());
        assertThat(deserialized.docIds(), arrayContaining(docIds));
        assertThat(deserialized.routings(), nullValue());
    }

    public void testRoutingsDroppedBeforeRoutingTransportVersion() throws IOException {
        final String[] docIds = { "1", "2" };
        final String[] routings = { "r1", "r2" };
        final EnsureDocsSearchableRequest request = new EnsureDocsSearchableRequest(
            "index",
            0,
            docIds,
            routings,
            SplitShardCountSummary.UNSET
        );

        // an older node neither writes nor reads routings, so they are silently dropped to null
        final TransportVersion beforeRouting = TransportVersionUtils.getPreviousVersion(ROUTING);
        final EnsureDocsSearchableRequest deserialized = copy(request, beforeRouting);
        assertThat(deserialized.docIds(), arrayContaining(docIds));
        assertThat(deserialized.routings(), nullValue());
    }

    private static EnsureDocsSearchableRequest copy(EnsureDocsSearchableRequest request, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new EnsureDocsSearchableRequest(in);
            }
        }
    }
}
