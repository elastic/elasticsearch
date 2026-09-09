/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Verifies that {@link ShardBatchMapper#resolveMappers} correctly recognizes {@link MatchOnlyTextFieldMapper}
 * as columnar-batch eligible (or not), mirroring the keyword coverage in
 * {@code org.elasticsearch.action.bulk.ShardBatchMapperResolveTests}. That test class lives in {@code server}'s
 * unit test source set, which has no dependency on {@code mapper-extras}, so match_only_text needs its own
 * equivalent here instead.
 */
public class MatchOnlyTextShardBatchMapperResolveTests extends MapperServiceTestCase {

    private final IndexSettings indexSettings = new IndexSettings(
        new IndexMetadata.Builder("index").settings(
            indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build()
        ).build(),
        Settings.builder().put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build()
    );

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    /** Builds a flat schema from simple (non-dotted) leaf names. */
    private static SourceSchema schemaOf(String... leafPaths) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            for (String path : leafPaths) {
                b.field(path, "v");
            }
            b.endObject();
            try (EscfBatch batch = EscfEncoder.encode(List.of(BytesReference.bytes(b)), XContentType.JSON)) {
                return batch.schema();
            }
        }
    }

    private MapperService mapper(XContentBuilder mapping) throws IOException {
        return createMapperService(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build(),
            mapping
        );
    }

    public void testMatchOnlyTextMapperIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("f").field("type", "match_only_text").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof MatchOnlyTextFieldMapper);
    }

    public void testMatchOnlyTextMultiValueFalseIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("f").field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof MatchOnlyTextFieldMapper);
    }

    public void testMatchOnlyTextWithMultiFieldsFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("f");
            b.field("type", "match_only_text");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings));
    }

    public void testMatchOnlyTextDocValuesDisabledFallsBack() throws IOException {
        MapperService ms = mapper(
            mapping(b -> { b.startObject("f").field("type", "match_only_text").field("doc_values", false).endObject(); })
        );
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings));
    }

    public void testMatchOnlyTextNonColumnarModeFallsBack() throws IOException {
        MapperService ms = createMapperService(mapping(b -> b.startObject("f").field("type", "match_only_text").endObject()));
        IndexSettings nonColumnar = ms.getIndexSettings();
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), nonColumnar);
        assertNull(resolution);
    }
}
