/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.FieldKnnVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class VectorFieldHintTests extends ESTestCase {

    private static final String FORMAT = "Lucene99HnswVectorsFormat";

    /** Two fields written by distinct format instances take separate suffixes, so each resolves. */
    public void testResolvesTheFieldOfEachSuffix() {
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { vectorField("first", 0, "0"), vectorField("second", 1, "1") });
        assertEquals(new VectorFieldHint("first"), VectorFieldHint.forSuffix(fieldInfos, FORMAT + "_0"));
        assertEquals(new VectorFieldHint("second"), VectorFieldHint.forSuffix(fieldInfos, FORMAT + "_1"));
    }

    /** A suffix covering several fields describes none of them, so it resolves to nothing. */
    public void testSharedSuffixResolvesToNothing() {
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { vectorField("first", 0, "0"), vectorField("second", 1, "0") });
        assertNull(VectorFieldHint.forSuffix(fieldInfos, FORMAT + "_0"));
    }

    public void testUnknownSuffixResolvesToNothing() {
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { vectorField("first", 0, "0") });
        assertNull(VectorFieldHint.forSuffix(fieldInfos, "NoSuchFormat_7"));
    }

    /** A vector field as {@link PerFieldKnnVectorsFormat} records it: the format it was written with and its suffix. */
    private static FieldInfo vectorField(String name, int number, String suffix) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY, FORMAT);
        attributes.put(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY, suffix);
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            attributes,
            0,
            0,
            0,
            2,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.DOT_PRODUCT,
            false,
            false
        );
    }

    /**
     * Two fields of one format get their per-field suffixes in a different order in each segment, and a merge numbers them
     * again: each merged raw vectors file still says the field it holds.
     */
    public void testAMergeSaysTheFieldOfEachMergedFile() throws Exception {
        Map<String, IOContext> rawCreates = new ConcurrentHashMap<>();
        Codec codec = new FilterCodec(Codec.getDefault().getName(), Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new PerFieldKnnVectorsFormat() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new FieldKnnVectorsFormat(field, new ES94HnswScalarQuantizedVectorsFormat());
                    }
                };
            }
        };
        try (Directory dir = new FilterDirectory(newDirectory()) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                if (name.endsWith(".vec") && context.context() == IOContext.Context.MERGE) {
                    rawCreates.put(name, context);
                }
                return super.createOutput(name, context);
            }
        }) {
            IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // the first segment meets b first, the second meets a first
                for (String first : List.of("b", "a")) {
                    String second = first.equals("a") ? "b" : "a";
                    for (int i = 0; i < 32; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField(first, randomFloatVector(16), VectorSimilarityFunction.EUCLIDEAN));
                        if (i > 0) {
                            doc.add(new KnnFloatVectorField(second, randomFloatVector(16), VectorSimilarityFunction.EUCLIDEAN));
                        }
                        writer.addDocument(doc);
                    }
                    writer.flush();
                }
                writer.forceMerge(1);
            }
            SegmentCommitInfo merged = SegmentInfos.readLatestCommit(dir).info(0);
            FieldInfos fieldInfos = codec.fieldInfosFormat().read(dir, merged.info, "", IOContext.READONCE);
            assertThat(rawCreates, aMapWithSize(2));
            for (String field : List.of("a", "b")) {
                FieldInfo info = fieldInfos.fieldInfo(field);
                String suffix = info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY)
                    + "_"
                    + info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
                String raw = IndexFileNames.segmentFileName(merged.info.name, suffix, "vec");
                assertThat(rawCreates, hasKey(raw));
                assertThat(field, rawCreates.get(raw).hints(VectorFieldHint.class).toList(), equalTo(List.of(new VectorFieldHint(field))));
            }
        }
    }
}
