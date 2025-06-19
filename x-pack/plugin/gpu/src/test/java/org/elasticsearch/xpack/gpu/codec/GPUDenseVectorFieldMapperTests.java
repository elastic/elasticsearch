/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.mapper.AbstractDenseVectorFieldMapperTestcase;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.gpu.GPUPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xpack.gpu.GPUPlugin.GPU_FORMAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GPUDenseVectorFieldMapperTests extends AbstractDenseVectorFieldMapperTestcase {

    @Before
    public void setup() {
        assumeTrue("feature flag [gpu_format] must be enabled", GPU_FORMAT.isEnabled());
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        var plugin = new GPUPlugin();
        return Collections.singletonList(plugin);
    }

    public void testGPUParsing() throws IOException {
        DocumentMapper mapperService = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 128);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "gpu");
            b.endObject();
        }));
        var denseVectorFieldMapper = (DenseVectorFieldMapper) mapperService.mappers().getMapper("field");
        var indexOptions = denseVectorFieldMapper.fieldType().getIndexOptions();
        var name = indexOptions.type().name();
        assertThat(name, equalTo("gpu"));
        // TODO: finish tests
    }

    public void testGPUParsingFailureInRelease() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                fieldMapping(
                    b -> b.field("type", "dense_vector").field("dims", dims).startObject("index_options").field("type", "gpu").endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("Unknown vector index options"));
    }

    public void testKnnGPUVectorsFormat() throws IOException {
        final int dims = randomIntBetween(64, 4096);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "gpu");
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);
        Codec codec = codecService.codec("default");
        KnnVectorsFormat knnVectorsFormat;
        if (CodecService.ZSTD_STORED_FIELDS_FEATURE_FLAG) {
            assertThat(codec, instanceOf(PerFieldMapperCodec.class));
            knnVectorsFormat = ((PerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        } else {
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
        }
        String expectedString = "GPUVectorsFormat()";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }
}
