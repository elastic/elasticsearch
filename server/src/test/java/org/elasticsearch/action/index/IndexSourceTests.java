/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.pooled;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexSourceTests extends ESTestCase {

    public void testNoRetentionForUnpooledSources() {
        final IndexSource fromBytes = new IndexSource();
        fromBytes.source(new BytesArray(randomAlphaOfLength(20)), randomFrom(XContentType.values()));
        assertThat(fromBytes.retainSourceRef(), nullValue());

        final IndexSource fromString = new IndexSource();
        fromString.source("{\"" + randomAlphaOfLength(5) + "\":\"" + randomAlphaOfLength(5) + "\"}", XContentType.JSON);
        assertThat(fromString.retainSourceRef(), nullValue());

        final IndexSource fromMap = new IndexSource();
        fromMap.source(Map.of(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        assertThat(fromMap.retainSourceRef(), nullValue());

        final IndexSource empty = new IndexSource();
        assertThat(empty.retainSourceRef(), nullValue());
    }

    /// Once the bytes have been copied into a shard level batch there is no inline source left to retain, so batched items must not
    /// contribute any retention.
    public void testNoRetentionOnceSourceMovedIntoBatch() throws IOException {
        final AtomicInteger releases = new AtomicInteger();
        final ReleasableBytesReference pooled = pooledSource(releases);

        final IndexSource indexSource = new IndexSource();
        indexSource.source(pooled, XContentType.JSON);
        try (SourceBatch batch = EscfEncoder.encode(List.of(new BytesArray("{\"field\":\"value\"}")), XContentType.JSON)) {
            indexSource.setSourceRow(batch, 0);
            assertThat(indexSource.retainSourceRef(), nullValue());
        }

        pooled.decRef();
        assertThat(releases.get(), equalTo(1));
    }

    public void testRetentionKeepsPooledBytesAliveAfterOwnerReleases() {
        final AtomicInteger releases = new AtomicInteger();
        final ReleasableBytesReference pooled = pooledSource(releases);
        final IndexSource indexSource = new IndexSource();
        indexSource.source(pooled, XContentType.JSON);

        Releasable retained = indexSource.retainSourceRef();
        assertThat(retained, notNullValue());

        pooled.decRef();
        assertTrue("the retained reference has to outlive the owner's release", pooled.hasReferences());
        assertThat(releases.get(), equalTo(0));

        retained.close();
        assertFalse(pooled.hasReferences());
        assertThat(releases.get(), equalTo(1));
    }

    public void testEachRetentionIsReleasedIndependently() {
        final AtomicInteger releases = new AtomicInteger();
        final ReleasableBytesReference pooled = pooledSource(releases);

        final IndexSource indexSource = new IndexSource();
        indexSource.source(pooled, XContentType.JSON);

        final int retentions = randomIntBetween(2, 5);
        final List<Releasable> retained = new ArrayList<>(retentions);
        for (int i = 0; i < retentions; i++) {
            retained.add(indexSource.retainSourceRef());
        }

        pooled.decRef();
        for (int i = 0; i < retentions; i++) {
            assertTrue("release [" + i + "] of [" + retentions + "] must not free the bytes", pooled.hasReferences());
            retained.get(i).close();
        }
        assertFalse(pooled.hasReferences());
        assertThat(releases.get(), equalTo(1));
    }

    /// A retention is a claim on the bytes it was taken against, not on whatever the source field happens to hold later.
    public void testRetentionSurvivesSourceReplacement() {
        final AtomicInteger releases = new AtomicInteger();
        final ReleasableBytesReference pooled = pooledSource(releases);

        final IndexSource indexSource = new IndexSource();
        indexSource.source(pooled, XContentType.JSON);
        final Releasable retained = indexSource.retainSourceRef();

        indexSource.source(new BytesArray(randomAlphaOfLength(20)), XContentType.JSON);
        assertThat("the replacement bytes are not pooled, so there is nothing left to retain", indexSource.retainSourceRef(), nullValue());

        pooled.decRef();
        assertTrue(pooled.hasReferences());
        retained.close();
        assertThat(releases.get(), equalTo(1));
    }

    private static ReleasableBytesReference pooledSource(AtomicInteger releases) {
        return pooled(new BytesArray("{\"field\":\"" + randomAlphaOfLength(20) + "\"}"), releases);
    }
}
