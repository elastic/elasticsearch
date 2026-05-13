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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.util.concurrent.EsExecutors.NODE_PROCESSORS_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class DiskBBQDenseVectorFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        Settings statelessSettings = Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
        return List.of(new TrialLicenseStateDiskBBQPlugin(statelessSettings));
    }

    public void testKnnBBQIVFVectorsFormat() throws IOException {
        final boolean enabled = randomBoolean();
        final Settings settings = IndexSettingsModule.newIndexSettings(
            "foo",
            Settings.builder().put(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey(), enabled).build()
        ).getSettings();
        final int dims = randomIntBetween(64, 4096);
        MapperService mapperService = createMapperService(getVersion(), settings, () -> true, fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "bbq_disk");
            b.endObject();
        }));
        try (var tp = new TestThreadPool(getTestName(), Settings.builder().put(NODE_PROCESSORS_SETTING.getKey(), 10).build())) {
            CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, tp);
            Codec codec = codecService.codec("default");
            if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
                codec = deduplicateFieldInfosCodec.delegate();
            }
            assertThat(codec, instanceOf(LegacyPerFieldMapperCodec.class));
            KnnVectorsFormat knnVectorsFormat = ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
            String expectedString = Build.current().isSnapshot()
                ? "ESNextDiskBBQVectorsFormat(vectorPerCluster=384, mergeExec=" + enabled + ", sliceField=null)"
                : "ES940DiskBBQVectorsFormat(vectorPerCluster=384, mergeExec=" + enabled + ")";
            assertEquals(expectedString, knnVectorsFormat.toString());
        }
    }

    public void testDefaultsToBBQDiskWhenLicensedOnStatelessNode() throws IOException {
        final int dims = randomIntBetween(1, 4096);
        MapperService mapperService = createMapperService(getVersion(), Settings.EMPTY, () -> true, fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", dims);
            b.field("index", true);
            b.field("similarity", "dot_product");
        }));

        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertNotNull(mapper);
        assertThat(mapper.fieldType().getIndexOptions(), instanceOf(DenseVectorFieldMapper.BBQIVFIndexOptions.class));
        assertEquals(DenseVectorFieldMapper.VectorIndexType.BBQ_DISK, mapper.fieldType().getIndexOptions().getType());
    }

    public void testDefaultsToBBQDiskInVectordbDocumentIndexMode() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), "vectordb_document").build();
        MapperService mapperService = createMapperService(
            getVersion(),
            settings,
            () -> true,
            fieldMapping(b -> b.field("type", "dense_vector"))
        );

        // Index the first document so dims gets set via dynamic mapping update;
        // default index_options selection is deferred until dims is configured.
        final int dims = randomIntBetween(1, 4096);
        final float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", vector)));
        mergeDynamicUpdate(mapperService, doc.dynamicMappingsUpdate());

        DenseVectorFieldMapper mapper = (DenseVectorFieldMapper) mapperService.mappingLookup().getMapper("field");
        assertNotNull(mapper);
        assertEquals(DenseVectorFieldMapper.ElementType.BFLOAT16, mapper.fieldType().getElementType());
        assertEquals(dims, mapper.fieldType().getVectorDimensions());
        assertThat(mapper.fieldType().getIndexOptions(), instanceOf(DenseVectorFieldMapper.BBQIVFIndexOptions.class));
        assertEquals(DenseVectorFieldMapper.VectorIndexType.BBQ_DISK, mapper.fieldType().getIndexOptions().getType());
    }

    public void testSliceSettingControlsSliceFieldForDiskBBQESNextFormat() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assumeTrue("ESNext DiskBBQ format is only used in snapshots", Build.current().isSnapshot());
        final Settings enabledSettings = IndexSettingsModule.newIndexSettings(
            "foo",
            Settings.builder()
                .put(IndexSettings.SLICE_ENABLED.getKey(), true)
                .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
                .build()
        ).getSettings();
        final Settings disabledSettings = IndexSettingsModule.newIndexSettings(
            "foo",
            Settings.builder()
                .put(IndexSettings.SLICE_ENABLED.getKey(), false)
                .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
                .build()
        ).getSettings();
        MapperService enabledMapperService = createMapperService(getVersion(), enabledSettings, () -> true, fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 64);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "bbq_disk");
            b.endObject();
        }));
        MapperService disabledMapperService = createMapperService(getVersion(), disabledSettings, () -> true, fieldMapping(b -> {
            b.field("type", "dense_vector");
            b.field("dims", 64);
            b.field("index", true);
            b.field("similarity", "dot_product");
            b.startObject("index_options");
            b.field("type", "bbq_disk");
            b.endObject();
        }));
        try (var tp = new TestThreadPool(getTestName())) {
            assertThat(
                knnVectorsFormatForField(enabledMapperService, tp).toString(),
                containsString("sliceField=" + RoutingFieldMapper.NAME)
            );
            assertThat(knnVectorsFormatForField(disabledMapperService, tp).toString(), containsString("sliceField=null"));
        }
    }

    private static KnnVectorsFormat knnVectorsFormatForField(MapperService mapperService, TestThreadPool tp) {
        CodecService codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE, tp);
        Codec codec = codecService.codec("default");
        if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
            codec = deduplicateFieldInfosCodec.delegate();
        }
        return ((LegacyPerFieldMapperCodec) codec).getKnnVectorsFormatForField("field");
    }

}
