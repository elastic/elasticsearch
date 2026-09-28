/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.codec.ElasticsearchStoredFieldsFormat.Mode;
import org.elasticsearch.index.codec.bwc.ES94TSDBBestCompressionLucene104Codec;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class ElasticsearchStoredFieldsFormatTests extends ESTestCase {

    private SegmentInfo segmentInfo(Directory dir, Map<String, String> attributes) {
        return new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            1,
            false,
            false,
            new Lucene104Codec(),
            Map.of(),
            StringHelper.randomId(),
            new HashMap<>(attributes),
            null
        );
    }

    public void testSegmentWithoutTheAttributeReadsAsLucene() throws Exception {
        try (Directory dir = newDirectory()) {
            SegmentInfo si = segmentInfo(dir, Map.of());
            assertNull(si.getAttribute(ElasticsearchStoredFieldsFormat.MODE_KEY));
            assertEquals(Mode.LUCENE, ElasticsearchStoredFieldsFormat.modeOf(si, Mode.LUCENE));
        }
    }

    /**
     * Segments written before the mode was recorded carry none, and which implementation wrote them depends on the codec name
     * they carry. Reading one with the wrong implementation would fail, so each codec has to answer for its own name.
     */
    public void testCodecsAnswerForSegmentsThatRecordNoMode() throws Exception {
        try (Directory dir = newDirectory()) {
            SegmentInfo noMode = segmentInfo(dir, Map.of());

            var elasticsearch96 = (ElasticsearchStoredFieldsFormat) ((TSDBStoredFieldsFormat) new Elasticsearch96Codec()
                .storedFieldsFormat()).delegate();
            assertEquals("Elasticsearch96 only ever wrote Lucene stored fields", Mode.LUCENE, elasticsearch96.modeOf(noMode));

            var es94 = (ElasticsearchStoredFieldsFormat) ((TSDBStoredFieldsFormat) new ES94TSDBBestCompressionLucene104Codec()
                .storedFieldsFormat()).delegate();
            assertEquals("ES94TSDBBestCompressionLucene104Codec wrote Zstd", Mode.ZSTD_BEST_COMPRESSION, es94.modeOf(noMode));
        }
    }

    public void testEveryModeRoundTripsThroughTheAttribute() throws Exception {
        try (Directory dir = newDirectory()) {
            for (Mode mode : Mode.values()) {
                SegmentInfo si = segmentInfo(dir, Map.of(ElasticsearchStoredFieldsFormat.MODE_KEY, mode.name()));
                assertEquals(mode, ElasticsearchStoredFieldsFormat.modeOf(si, Mode.LUCENE));
            }
        }
    }

    public void testUnknownModeIsRejected() throws Exception {
        try (Directory dir = newDirectory()) {
            SegmentInfo si = segmentInfo(dir, Map.of(ElasticsearchStoredFieldsFormat.MODE_KEY, "NOT_A_MODE"));
            var e = expectThrows(IllegalStateException.class, () -> ElasticsearchStoredFieldsFormat.modeOf(si, Mode.LUCENE));
            assertThat(e.getMessage(), containsString("unknown stored fields mode [NOT_A_MODE]"));
        }
    }

    public void testWritingTwoModesIntoOneSegmentIsRejected() throws Exception {
        try (Directory dir = newDirectory()) {
            SegmentInfo si = segmentInfo(dir, Map.of(ElasticsearchStoredFieldsFormat.MODE_KEY, Mode.ZSTD_BEST_COMPRESSION.name()));
            var format = new ElasticsearchStoredFieldsFormat(Mode.LUCENE, Mode.LUCENE, new Lucene104Codec().storedFieldsFormat());
            var e = expectThrows(IllegalStateException.class, () -> format.fieldsWriter(dir, si, IOContext.DEFAULT));
            assertThat(e.getMessage(), containsString("cannot also write it as [LUCENE]"));
        }
    }

    public void testTheModeIsRecordedInSegmentsOnDisk() throws Exception {
        Codec codec = new Elasticsearch96Codec(Lucene104Codec.Mode.BEST_SPEED);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
                Document doc = new Document();
                doc.add(new StoredField("field", "value"));
                w.addDocument(doc);
            }
            SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
            assertThat(sis.size(), greaterThan(0));
            for (SegmentCommitInfo sci : sis) {
                assertEquals(
                    "segment [" + sci.info.name + "] must record the mode it was written with",
                    Mode.LUCENE.name(),
                    sci.info.getAttribute(ElasticsearchStoredFieldsFormat.MODE_KEY)
                );
            }
        }
    }
}
