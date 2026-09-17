/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

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
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Verifies that {@link ShardBatchMapper#resolveMappers} correctly recognizes {@link WildcardFieldMapper}
 * as columnar-batch eligible (or not). Modelled after
 * {@code org.elasticsearch.index.mapper.extras.MatchOnlyTextShardBatchMapperResolveTests}.
 */
public class WildcardShardBatchMapperResolveTests extends MapperServiceTestCase {

    private final IndexSettings indexSettings = new IndexSettings(
        new IndexMetadata.Builder("index").settings(
            indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build()
        ).build(),
        Settings.builder().put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build()
    );

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new Wildcard());
    }

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

    private MapperService columnarMapper(XContentBuilder mapping) throws IOException {
        return createMapperService(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build(),
            mapping
        );
    }

    public void testWildcardMapperIsSupported() throws IOException {
        MapperService ms = columnarMapper(mapping(b -> { b.startObject("f").field("type", "wildcard").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof WildcardFieldMapper);
    }

    public void testWildcardWithKeywordSubFieldResolves() throws IOException {
        MapperService ms = columnarMapper(mapping(b -> {
            b.startObject("f");
            b.field("type", "wildcard");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull("wildcard with a keyword sub-field must take the columnar fast path", resolution);
        assertTrue(resolution.columnMappers()[0] instanceof WildcardFieldMapper);
    }

    public void testKeywordParentWithWildcardSubFieldResolves() throws IOException {
        MapperService ms = columnarMapper(mapping(b -> {
            b.startObject("f");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("wc").field("type", "wildcard").endObject();
            b.endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), indexSettings);
        assertNotNull("keyword parent with wildcard sub-field must take the columnar fast path", resolution);
    }

    public void testWildcardNonColumnarModeFallsBack() throws IOException {
        MapperService ms = createMapperService(mapping(b -> b.startObject("f").field("type", "wildcard").endObject()));
        IndexSettings nonColumnar = ms.getIndexSettings();
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("f"), ms.mappingLookup(), nonColumnar);
        assertNull(resolution);
    }
}
