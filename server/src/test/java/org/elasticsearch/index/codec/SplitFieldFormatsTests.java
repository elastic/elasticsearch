/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * End-to-end checks that {@code index.codec.per_field_files} explodes a segment so each field has its own files (rather
 * than co-mingling fields in shared files or bundling everything into a {@code .cfs}), while remaining fully readable.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // keep listAll() free of stray files so per-field file counts are exact
public class SplitFieldFormatsTests extends ESTestCase {

    private static final String[] DV_FIELDS = { "dv_a", "dv_b", "dv_c" };
    private static final int NUM_DOCS = 5;

    public void testSplitSegmentHasOneFileSetPerField() throws IOException {
        try (Directory dir = newDirectory()) {
            writeDocs(dir, true);
            String[] files = dir.listAll();
            // Each doc-values field is written to its own .dvd/.dvm pair instead of co-mingling in shared files.
            assertEquals("expected one .dvd per field", DV_FIELDS.length, countSuffix(files, ".dvd"));
            assertEquals("expected one .dvm per field", DV_FIELDS.length, countSuffix(files, ".dvm"));
            assertRoundTrip(dir);
        }
    }

    public void testNonSplitSegmentCoMinglesFieldsInSharedFiles() throws IOException {
        try (Directory dir = newDirectory()) {
            writeDocs(dir, false);
            String[] files = dir.listAll();
            // Without per-field files all doc-values fields share a single .dvd/.dvm pair.
            assertEquals("expected a single shared .dvd", 1, countSuffix(files, ".dvd"));
            assertEquals("expected a single shared .dvm", 1, countSuffix(files, ".dvm"));
            assertRoundTrip(dir);
        }
    }

    private void writeDocs(Directory dir, boolean split) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(createCodec(split));
        // Keep compound files off in both cases so the per-field file layout is observable on disk. In production the
        // compound_format threshold still bundles small segments; per-field files only stay loose for larger segments.
        iwc.setUseCompoundFile(false);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int d = 0; d < NUM_DOCS; d++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(d), Field.Store.NO));
                for (String field : DV_FIELDS) {
                    doc.add(new NumericDocValuesField(field, value(field, d)));
                }
                w.addDocument(doc);
            }
            w.commit();
        }
    }

    private Codec createCodec(boolean split) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD)
            .put(IndexSettings.INDEX_PER_FIELD_FILES_SETTING.getKey(), split)
            .build();
        MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), settings, "test");
        mapperService.merge("type", new CompressedXContent("{\"properties\":{}}"), MapperService.MergeReason.MAPPING_UPDATE);
        return new PerFieldMapperCodec(Zstd814StoredFieldsFormat.Mode.BEST_SPEED, mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
    }

    private void assertRoundTrip(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            for (String field : DV_FIELDS) {
                int seen = 0;
                for (LeafReaderContext leaf : reader.leaves()) {
                    NumericDocValues dv = leaf.reader().getNumericDocValues(field);
                    assertNotNull("missing doc values for [" + field + "]", dv);
                    for (int doc = dv.nextDoc(); doc != NumericDocValues.NO_MORE_DOCS; doc = dv.nextDoc()) {
                        assertEquals("value mismatch for [" + field + "]", value(field, doc), dv.longValue());
                        seen++;
                    }
                }
                assertEquals("unexpected doc count for [" + field + "]", NUM_DOCS, seen);
            }
        }
    }

    private static long value(String field, int doc) {
        return (long) field.hashCode() * 31 + doc;
    }

    private static long countSuffix(String[] files, String suffix) {
        return Arrays.stream(files).filter(f -> f.endsWith(suffix)).count();
    }
}
