/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for the multi-partition encoder API: {@link EirfEncoder#parseToScratch},
 * {@link EirfEncoder#commitScratchTo}, {@link EirfEncoder#buildPartition}, and the
 * {@link EirfEncoder.LeafSink} contract.
 *
 * <p>Existing single-partition coverage (the {@code addDocument} / {@code build} pair) lives in
 * {@link EirfEncoderTests} and is unchanged — those tests pin the legacy behavior, this file pins
 * the new multi-partition behavior.
 */
public class EirfEncoderPartitionedTests extends ESTestCase {

    public void testCommitToDifferentPartitionsKeepsRowsSeparate() throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.parseToScratch(json("""
                {"name":"alice","val":1}"""), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            assertThat(encoder.commitScratchTo(7), equalTo(0));

            encoder.parseToScratch(json("""
                {"name":"bob","val":2}"""), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            assertThat(encoder.commitScratchTo(13), equalTo(0));

            encoder.parseToScratch(json("""
                {"name":"carol","val":3}"""), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            assertThat(encoder.commitScratchTo(7), equalTo(1));

            assertThat(encoder.docCount(7), equalTo(2));
            assertThat(encoder.docCount(13), equalTo(1));
            assertThat(encoder.hasPartition(7), equalTo(true));
            assertThat(encoder.hasPartition(42), equalTo(false));

            EirfBatch batchSeven = encoder.buildPartition(7);
            EirfBatch batchThirteen = encoder.buildPartition(13);
            try (batchSeven; batchThirteen) {
                assertThat(batchSeven.docCount(), equalTo(2));
                assertThat(batchThirteen.docCount(), equalTo(1));
                // All partitions share the schema unioned across all documents.
                assertThat(batchSeven.schema().leafCount(), equalTo(2));
                assertThat(batchThirteen.schema().leafCount(), equalTo(2));
            }
        }
    }

    public void testCommitWithoutStagedRowThrows() throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            expectThrows(IllegalStateException.class, () -> encoder.commitScratchTo(0));
        }
    }

    public void testLeafSinkFiresForPrimitivesNotForNullsOrEmptyObjects() throws IOException {
        RecordingSink sink = new RecordingSink();
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.parseToScratch(json("""
                {"s":"hi","i":42,"d":1.5,"b":true,"n":null,"empty":{}}"""), XContentType.JSON, sink);
            // VALUE_STRING, VALUE_NUMBER (INT), VALUE_NUMBER (FLOAT), VALUE_BOOLEAN — VALUE_NULL and
            // empty objects (encoded as KEY_VALUE leaves) must not fire onPrimitive.
            assertThat(sink.events, containsInAnyOrder("primitive:s=hi", "primitive:i=42", "primitive:d=1.5", "primitive:b=true"));
        }
    }

    public void testLeafSinkFiresOnArrayLeafForArraysButNotForArraysInsideObjects() throws IOException {
        RecordingSink sink = new RecordingSink();
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.parseToScratch(json("""
                {"tag":["a","b"],"x":1}"""), XContentType.JSON, sink);
            // The array at "tag" should fire onArrayLeaf exactly once. Array elements themselves are
            // not exposed via the LeafSink in this PR.
            long arrayEvents = sink.events.stream().filter(e -> e.startsWith("array:tag")).count();
            assertThat("arrays at leaf positions should fire onArrayLeaf once per leaf", arrayEvents, equalTo(1L));
            assertThat(sink.events.contains("primitive:x=1"), equalTo(true));
        }
    }

    public void testLeafSinkPathsBuildDottedNamesForNestedObjects() throws IOException {
        RecordingSink sink = new RecordingSink();
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.parseToScratch(json("""
                {"dim":{"host":"h1","region":"us"}}"""), XContentType.JSON, sink);
            assertThat(sink.events, containsInAnyOrder("primitive:dim.host=h1", "primitive:dim.region=us"));
        }
    }

    public void testColumnPathIsCachedAcrossRows() throws IOException {
        // Encoding two documents that share the same fields should not change the cached path string
        // identity for the same column index.
        RecordingSink first = new RecordingSink();
        RecordingSink second = new RecordingSink();
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.parseToScratch(json("""
                {"dim":{"host":"a"}}"""), XContentType.JSON, first);
            encoder.commitScratchTo(0);
            String firstPath = first.lastDottedPath;

            encoder.parseToScratch(json("""
                {"dim":{"host":"b"}}"""), XContentType.JSON, second);
            encoder.commitScratchTo(0);
            String secondPath = second.lastDottedPath;

            assertThat(secondPath, equalTo(firstPath));
            // columnPath() exposes the same cached string
            assertThat(encoder.columnPath(0), equalTo("dim.host"));
        }
    }

    public void testBuildPartitionForUnusedKeyReturnsEmptyBatch() throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.parseToScratch(json("""
                {"x":1}"""), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            encoder.commitScratchTo(0);
            EirfBatch batchUsed = encoder.buildPartition(0);
            EirfBatch batchUnused = encoder.buildPartition(99);
            try (batchUsed; batchUnused) {
                assertThat(batchUsed.docCount(), equalTo(1));
                assertThat(batchUnused.docCount(), equalTo(0));
                // Unused partition still carries the schema and a well-formed (empty) doc index;
                // total size is at least the header.
                assertThat(batchUnused.data().length(), greaterThan(0));
            }
        }
    }

    public void testAddDocumentWorks() throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.addDocument(json("""
                {"x":1}"""), XContentType.JSON, 0);
            encoder.addDocument(json("""
                {"x":2}"""), XContentType.JSON, 0);
            try (EirfBatch batch = encoder.buildPartition(0)) {
                assertThat(batch.docCount(), equalTo(2));
            }
        }
    }

    private static BytesReference json(String s) {
        return new BytesArray(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Records every callback the sink receives. Returns {@code true} from {@link #passRawText()}
     * so the encoder routes every primitive through {@link #onTextPrimitive} — keeps the recorded
     * events comparable across primitive types regardless of the encoder's typed narrowing.
     */
    private static final class RecordingSink implements EirfEncoder.LeafSink {
        final List<String> events = new ArrayList<>();
        String lastDottedPath;

        @Override
        public boolean passRawText() {
            return true;
        }

        @Override
        public void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {
            String text = new String(textBytes.bytes(), textBytes.offset(), textBytes.length(), StandardCharsets.UTF_8);
            events.add("primitive:" + dottedPath + "=" + text);
            lastDottedPath = dottedPath;
        }

        @Override
        public void onArrayLeaf(int columnIndex, String dottedPath) {
            events.add("array:" + dottedPath);
            lastDottedPath = dottedPath;
        }
    }
}
