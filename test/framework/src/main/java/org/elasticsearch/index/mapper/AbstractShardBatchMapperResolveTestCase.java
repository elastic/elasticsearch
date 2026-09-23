/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract base for tests that verify {@link ShardBatchMapper#resolveMappers} correctly recognizes
 * field mappers as columnar-batch eligible (or not).
 *
 * <p>Subclasses write {@code public void testXxx()} methods and call
 * {@link ShardBatchMapper#resolveMappers} directly. Subclasses that test out-of-server field types
 * must override {@link MapperServiceTestCase#getPlugins()} to include the relevant plugin.
 *
 * <p>Note: {@link #indexSettings} is a <em>synthetic</em> {@link IndexSettings} object built for
 * the purpose of passing to {@link ShardBatchMapper#resolveMappers}; it is independent of
 * {@link MapperService#getIndexSettings()} on any particular service instance. Fall-back tests that
 * intentionally use non-columnar settings pass {@code ms.getIndexSettings()} directly rather than
 * this field.
 */
public abstract class AbstractShardBatchMapperResolveTestCase extends MapperServiceTestCase {

    /**
     * Strict-columnar {@link IndexSettings} with recovery source disabled — the settings
     * {@link ShardBatchMapper#resolveMappers} is normally called with.
     */
    protected final IndexSettings indexSettings = new IndexSettings(
        new IndexMetadata.Builder("index").settings(
            indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build()
        ).build(),
        Settings.builder().put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build()
    );

    /**
     * Returns {@link #indexSettings} augmented with {@code extra}, for cases that need routing
     * paths, the unmapped sink, or other per-test settings variants.
     */
    protected static IndexSettings columnarIndexSettings(Settings extra) {
        return new IndexSettings(
            new IndexMetadata.Builder("index").settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put(extra)
                    .build()
            ).build(),
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build()
        );
    }

    /**
     * Creates a {@link MapperService} configured for strict-columnar mode with recovery source
     * disabled, using the given mapping.
     */
    protected final MapperService columnarMapperService(XContentBuilder mapping) throws IOException {
        return columnarMapperService(Settings.EMPTY, mapping);
    }

    /**
     * Creates a {@link MapperService} configured for strict-columnar mode with recovery source
     * disabled, additionally applying {@code extra} settings.
     */
    protected final MapperService columnarMapperService(Settings extra, XContentBuilder mapping) throws IOException {
        return createMapperService(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .put(extra)
                .build(),
            mapping
        );
    }

    /**
     * Builds a flat {@link SourceSchema} from simple (non-dotted) leaf names. The leaf values are
     * arbitrary — {@link ShardBatchMapper#resolveMappers} reads paths only, not values.
     */
    protected static SourceSchema schemaOf(String... leafPaths) throws IOException {
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

    /**
     * Builds a {@link SourceSchema} from dotted paths (e.g. {@code "outer.inner"}), converting
     * each to a nested JSON object. Supports one level of nesting only.
     */
    @SuppressWarnings("unchecked")
    protected static SourceSchema schemaOfNested(String... dottedPaths) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        for (String path : dottedPaths) {
            int dot = path.indexOf('.');
            String parent = path.substring(0, dot);
            String child = path.substring(dot + 1);
            Map<String, Object> nested = (Map<String, Object>) doc.computeIfAbsent(parent, k -> new LinkedHashMap<>());
            nested.put(child, "v");
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            try (EscfBatch batch = EscfEncoder.encode(List.of(BytesReference.bytes(b.map(doc))), XContentType.JSON)) {
                return batch.schema();
            }
        }
    }

    /**
     * Builds a {@link SourceSchema} from one or more raw JSON source documents. The schema is the
     * union of all leaves across documents, in the order they are first encountered.
     */
    protected static SourceSchema schemaOfJson(String... jsonDocs) throws IOException {
        final List<BytesReference> bytes = Arrays.stream(jsonDocs).map(s -> (BytesReference) new BytesArray(s)).toList();
        try (EscfBatch batch = EscfEncoder.encode(bytes, XContentType.JSON)) {
            return batch.schema();
        }
    }

    /**
     * Builds an {@link EscfBatch} from a raw JSON string. The caller is responsible for closing the
     * returned batch. Use this only when tests need real column data (e.g. to exercise
     * {@link EscfBatch#isEmptyObjectColumn}).
     */
    protected static EscfBatch batchOfJson(String json) throws IOException {
        return EscfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);
    }
}
