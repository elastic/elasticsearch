/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class DiskBBQDenseVectorFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TrialLicenseStateDiskBBQPlugin(Settings.EMPTY));
    }

    public void testKnnBBQIVFVectorsFormat() throws IOException {
        final int dims = randomIntBetween(64, 4096);
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "bbq_disk");
            b.endObject();
        }));
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);
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
        String expectedString = Build.current().isSnapshot()
            ? "ESNextDiskBBQVectorsFormat(vectorPerCluster=384)"
            : "ES920DiskBBQVectorsFormat(vectorPerCluster=384)";
        assertEquals(expectedString, knnVectorsFormat.toString());
    }

}
