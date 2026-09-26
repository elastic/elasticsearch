/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.index.codec.vectors.ES814HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutEntitlements;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Verifies that GPU HNSW format names match their CPU equivalents, so that
 * Lucene's {@link PerFieldKnnVectorsFormat} resolves the correct reader at
 * search time via {@link KnnVectorsFormat#forName(String)}.
 */
@WithoutEntitlements
public class GpuFormatNameCompatibilityTests extends ESTestCase {

    static {
        LogConfigurator.configureESLogging();
    }

    static boolean gpuSupported;

    @BeforeClass
    public static void beforeClass() {
        gpuSupported = CuVSGPUSupport.instance().isSupported();
    }

    public void testGpuFloatFormatName() {
        assertEquals("Lucene99HnswVectorsFormat", ES92GpuHnswVectorsFormat.NAME);
    }

    public void testGpuSQFormatNameMatchesCpuSQ() {
        assertEquals("ES814HnswScalarQuantizedVectorsFormat", ES92GpuHnswSQVectorsFormat.NAME);
        assertEquals(ES814HnswScalarQuantizedVectorsFormat.NAME, ES92GpuHnswSQVectorsFormat.NAME);
    }

    // -- the remainder of the tests require a GPU to be present

    public void testGpuFloatFormatNameFromFormat() {
        assumeTrue("cuvs not supported", gpuSupported);
        assertEquals("Lucene99HnswVectorsFormat", (new ES92GpuHnswVectorsFormat()).getName());
    }

    public void testGpuSQFormatNameMatchesCpuSQFromFormat() {
        assumeTrue("cuvs not supported", gpuSupported);
        assertEquals("ES814HnswScalarQuantizedVectorsFormat", (new ES92GpuHnswSQVectorsFormat()).getName());
    }

    public void testForNameResolvesCpuSQFormat() {
        assumeTrue("cuvs not supported", gpuSupported);
        var resolved = KnnVectorsFormat.forName(ES92GpuHnswSQVectorsFormat.NAME);
        assertNotNull(resolved);
        assertEquals("ES814HnswScalarQuantizedVectorsFormat", resolved.getName());
    }

    public void testFloatFieldInfoFormatNameAfterIndexing() throws IOException {
        assumeTrue("cuvs not supported", gpuSupported);
        doTestFieldInfoFormatName(new ES92GpuHnswVectorsFormat(), "Lucene99HnswVectorsFormat");
    }

    public void testSQFieldInfoFormatNameAfterIndexing() throws IOException {
        assumeTrue("cuvs not supported", gpuSupported);
        doTestFieldInfoFormatName(new ES92GpuHnswSQVectorsFormat(), "ES814HnswScalarQuantizedVectorsFormat");
    }

    private void doTestFieldInfoFormatName(KnnVectorsFormat gpuFormat, String expectedFormatName) throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(gpuFormat));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 1.0f, 2.0f, 3.0f }, VectorSimilarityFunction.DOT_PRODUCT));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    FieldInfo fi = leaf.reader().getFieldInfos().fieldInfo("vector");
                    assertNotNull(fi);
                    String storedFormatName = fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
                    assertEquals(
                        "Format name written to FieldInfo must match CPU equivalent for correct SPI resolution",
                        expectedFormatName,
                        storedFormatName
                    );
                }
            }
        }
    }
}
