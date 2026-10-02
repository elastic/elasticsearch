/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SampledQueryTests extends ESTestCase {

    private static final AttachmentKey<String> LABEL = new AttachmentKey<>("label", String.class);
    // same name and type as LABEL on purpose: keys are told apart by identity, not by name
    private static final AttachmentKey<String> OTHER_LABEL = new AttachmentKey<>("label", String.class);

    public void testAttachmentsAreEmptyUntilAttached() {
        SampledQuery query = sampled();
        assertThat(query.attachment(LABEL), nullValue());
        assertThat(query.groundTruth(), nullValue());
    }

    public void testAttachmentsAreKeptPerKey() {
        SampledQuery query = sampled();

        query.attach(LABEL, "a");
        query.attach(OTHER_LABEL, "b");

        assertThat(query.attachment(LABEL), equalTo("a"));
        assertThat(query.attachment(OTHER_LABEL), equalTo("b"));
        assertThat("other payloads are not affected", query.groundTruth(), nullValue());
    }

    public void testAttachingAgainReplacesThePayload() {
        SampledQuery query = sampled();
        query.attach(LABEL, "a");
        query.attach(LABEL, "b");
        assertThat(query.attachment(LABEL), equalTo("b"));
    }

    public void testGroundTruthIsAnAttachment() {
        SampledQuery query = sampled();
        GroundTruth groundTruth = new GroundTruth(List.of());

        query.groundTruth(groundTruth);

        assertThat(query.attachment(GroundTruth.KEY), sameInstance(groundTruth));
        assertThat(query.groundTruth(), sameInstance(groundTruth));
    }

    private static SampledQuery sampled() {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        return new SampledQuery(new QueryFingerprint(1, 1), new CapturedSearch(query, List.of(), 1), new TrackedQuery());
    }
}
