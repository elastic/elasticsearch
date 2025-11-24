/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTests;
import org.elasticsearch.plugins.Plugin;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;

public class GPUDenseVectorFieldMapperTests extends DenseVectorFieldMapperTests {

    @BeforeClass
    public static void setup() {
        assumeTrue("cuvs not supported", GPUSupport.isSupported());
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        var plugin = new GPUPlugin();
        return Collections.singletonList(plugin);
    }

    @Override
    public void testKnnVectorsFormat() throws IOException {
        // TODO improve test with custom parameters
        KnnVectorsFormat knnVectorsFormat = getKnnVectorsFormat("hnsw");
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=12, beamWidth=22, flatVectorFormat=Lucene99FlatVectorsFormat)";
        assertEquals(expectedStr, knnVectorsFormat.toString());
    }

    @Override
    public void testKnnQuantizedHNSWVectorsFormat() throws IOException {
        // TOD improve the test with custom parameters
        KnnVectorsFormat knnVectorsFormat = getKnnVectorsFormat("int8_hnsw");
        String expectedStr = "Lucene99HnswVectorsFormat(name=Lucene99HnswVectorsFormat, "
            + "maxConn=12, beamWidth=22, flatVectorFormat=ES814ScalarQuantizedVectorsFormat";
        assertTrue(knnVectorsFormat.toString().startsWith(expectedStr));
    }

    private KnnVectorsFormat getKnnVectorsFormat(String indexOptionsType) throws IOException {
        final int dims = randomIntBetween(128, 4096);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", indexOptionsType);
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            return ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            return ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
    }
}
