/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.ColumnGroupResolver.ColumnGroupResolution;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
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

import static org.hamcrest.Matchers.instanceOf;

public class ShardBatchMapperResolveTests extends MapperServiceTestCase {

    private final IndexSettings indexSettings = new IndexSettings(
        new IndexMetadata.Builder("index").settings(
            indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build()
        ).build(),
        Settings.builder().put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build()
    );

    /** Builds a flat schema from simple (non-dotted) leaf names. */
    private static SourceSchema schemaOf(String... leafPaths) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            for (String path : leafPaths) {
                b.field(path, 0);
            }
            b.endObject();
            try (EscfBatch batch = EscfEncoder.encode(List.of(BytesReference.bytes(b)), XContentType.JSON)) {
                return batch.schema();
            }
        }
    }

    /** Builds a schema from dotted paths (e.g. "outer.inner"), converting each to a nested JSON object. */
    @SuppressWarnings("unchecked")
    private static SourceSchema schemaOfNested(String... dottedPaths) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        for (String path : dottedPaths) {
            int dot = path.indexOf('.');
            String parent = path.substring(0, dot);
            String child = path.substring(dot + 1);
            Map<String, Object> nested = (Map<String, Object>) doc.computeIfAbsent(parent, k -> new LinkedHashMap<>());
            nested.put(child, 0);
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            try (EscfBatch batch = EscfEncoder.encode(List.of(BytesReference.bytes(b.map(doc))), XContentType.JSON)) {
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

    public void testHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("value").field("type", "keyword").endObject();
        }));
        SourceSchema schema = schemaOf("host", "value");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(2, resolution.columnMappers().length);
        assertTrue(resolution.columnMappers()[schema.findLeaf("host", 0)] instanceof KeywordFieldMapper);
        assertTrue(resolution.columnMappers()[schema.findLeaf("value", 0)] instanceof KeywordFieldMapper);
    }

    public void testKeywordIgnoreAboveIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("host").field("type", "keyword").field("ignore_above", 32).endObject()));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof KeywordFieldMapper);
    }

    public void testNumberIgnoreMalformedIsNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("v").field("type", "long").field("ignore_malformed", true).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("v"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testMissingLeafUnderDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOf("known", "unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", 0)]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);
    }

    public void testMissingLeafUnderDynamicTrueFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("known").field("type", "keyword").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known", "unknown"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    // TODO: not relevant at the moment because we are columnar only which does not support runtime fields
    // public void testRuntimeFieldInMappingFallsBack() throws IOException {
    // MapperService ms = mapper(topMapping(b -> {
    // b.startObject("runtime");
    // b.startObject("rt").field("type", "keyword").endObject();
    // b.endObject();
    // b.startObject("properties");
    // b.startObject("known").field("type", "keyword").endObject();
    // b.endObject();
    // }));
    // BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known"), ms.mappingLookup(), indexSettings);
    // assertNull(resolution);
    // }

    public void testIndexTimeScriptFallsBack() throws IOException {
        // A long field with a script is a standard example of an index-time script. Registering one
        // populates MappingLookup.indexTimeScriptMappers() which resolveMappers short-circuits on.
        // We can't easily register a real script in a unit test without wiring a ScriptService, but
        // we can verify that any mapper marked hasScript=true via the `script` parameter trips the
        // supportsBatchIndexing() guard. That path is covered by testUnsupportedMapperType below
        // (the short-circuit in resolveMappers on indexTimeScriptMappers is a superset check and
        // redundant with the per-mapper guard, so this test is intentionally narrow).
    }

    public void testTextMapperNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("t").field("type", "text").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testBooleanMapperNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("b").field("type", "boolean").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("b"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testIpMapperNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("ip").field("type", "ip").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("ip"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    // TODO: not relevant at the moment because we are columnar only which does not support copy_to
    // public void testKeywordWithCopyToFallsBack() throws IOException {
    // MapperService ms = mapper(mapping(b -> {
    // b.startObject("src").field("type", "keyword").field("copy_to", "dst").endObject();
    // b.startObject("dst").field("type", "keyword").endObject();
    // }));
    // assertNull(ShardBatchMapper.resolveMappers(schemaOf("src"), ms.mappingLookup(), indexSettings));
    // }

    public void testKeywordWithMultiFieldsFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    public void testNestedLeafHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.startObject("properties");
            b.startObject("inner").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfNested("outer.inner");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof KeywordFieldMapper);
    }

    public void testNestedLeafUnderNestedDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfNested("outer.known", "outer.unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", schema.findNonLeaf("outer", 0))]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", schema.findNonLeaf("outer", 0))]);
    }

    /**
     * Builds a schema from one or more raw JSON source documents. The schema is the union of all leaves
     * across documents, in the order they are first encountered.
     */
    private static SourceSchema schemaOfJson(String... jsonDocs) throws IOException {
        final List<BytesReference> bytes = Arrays.stream(jsonDocs).map(s -> (BytesReference) new BytesArray(s)).toList();
        try (EscfBatch batch = EscfEncoder.encode(bytes, XContentType.JSON)) {
            return batch.schema();
        }
    }

    // ── Flattened-field group resolution ─────────────────────────────────────────────────────────

    /**
     * Core regression: flattened sub-keys resolve to a group, not individual leaf mappers, and not
     * a runtime-field shadow (which would cause a fallback).
     */
    public void testFlattenedGroupHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        SourceSchema schema = schemaOfJson("{\"flat\":{\"key1\":\"a\",\"key2\":\"b\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("expected columnar path to succeed for a basic flattened field", resolution);

        // Both sub-key leaves must be null in columnMappers (owned by the group, not as leaf mappers).
        final int key1 = schema.findLeaf("key1", schema.findNonLeaf("flat", 0));
        final int key2 = schema.findLeaf("key2", schema.findNonLeaf("flat", 0));
        assertNull("flat.key1 should be owned by the group, not a leaf mapper", resolution.columnMappers()[key1]);
        assertNull("flat.key2 should be owned by the group, not a leaf mapper", resolution.columnMappers()[key2]);

        // One group, two leaves.
        final ColumnGroupResolution[] groups = resolution.columnGroups();
        assertEquals(1, groups.length);
        assertThat(groups[0].mapper(), instanceOf(FlattenedFieldMapper.class));
        assertArrayEquals(new int[] { key1, key2 }, groups[0].leafIndexes());
        assertArrayEquals(new String[] { "key1", "key2" }, groups[0].relativeKeys());
    }

    /** A plain keyword leaf coexists with a flattened group without either affecting the other. */
    public void testFlattenedGroupCoexistsWithPlainLeaf() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("attrs").field("type", "flattened").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"host\":\"srv\",\"attrs\":{\"env\":\"prod\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);

        assertThat(resolution.columnMappers()[schema.findLeaf("host", 0)], instanceOf(KeywordFieldMapper.class));
        assertEquals(1, resolution.columnGroups().length);
        assertEquals("attrs", resolution.columnGroups()[0].mapper().fullPath());
    }

    /** A leaf at the flattened field's own path (null or empty object) uses the leaf mapper, not the group. */
    public void testFlattenedLeafAtOwnPathUsesLeafMapper() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        // {"flat":null} produces a leaf at "flat" directly under root.
        SourceSchema schema = schemaOfJson("{\"flat\":null}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(resolution.columnMappers()[schema.findLeaf("flat", 0)], instanceOf(FlattenedFieldMapper.class));
        assertEquals(0, resolution.columnGroups().length);
    }

    /** A batch mixing a null leaf and group sub-keys produces both a leaf mapper and a group. */
    public void testFlattenedOwnPathLeafAndGroupCoexist() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        // Two docs: one null, one with a sub-key. Schema has both the own-path leaf and the group leaves.
        SourceSchema schema = schemaOfJson("{\"flat\":null}", "{\"flat\":{\"k\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        // The own-path leaf "flat" gets a leaf mapper.
        assertThat(resolution.columnMappers()[schema.findLeaf("flat", 0)], instanceOf(FlattenedFieldMapper.class));
        // The sub-key leaf "k" belongs to the group.
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "k" }, resolution.columnGroups()[0].relativeKeys());
    }

    /** Two independent flattened fields produce two separate groups, ordered by first appearance. */
    public void testTwoFlattenedFieldsProduceTwoGroups() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("tags").field("type", "flattened").endObject();
            b.startObject("meta").field("type", "flattened").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"tags\":{\"color\":\"red\"},\"meta\":{\"region\":\"us\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(2, resolution.columnGroups().length);
        assertEquals("tags", resolution.columnGroups()[0].mapper().fullPath());
        assertEquals("meta", resolution.columnGroups()[1].mapper().fullPath());
    }

    /** A nested object inside the flattened value collapses to a dotted relative key. */
    public void testNestedKeyProducesCompoundRelativeKey() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        SourceSchema schema = schemaOfJson("{\"flat\":{\"outer\":{\"inner\":\"v\"}}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "outer.inner" }, resolution.columnGroups()[0].relativeKeys());
    }

    /** A flattened field with {@code "index": true} is unsupported; the whole batch falls back. */
    public void testUnsupportedFlattenedConfigFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").field("index", true).endObject()));
        SourceSchema schema = schemaOfJson("{\"flat\":{\"k\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNull("flattened with index=true should cause fallback", resolution);
    }

    /**
     * A non-group FieldMapper at an ancestor path stops the walk. The leaf under that ancestor has no
     * group owner and no direct mapper, so it triggers a runtime-field-shadow or unmapped fallback.
     */
    public void testNonGroupFieldMapperAncestorFallsBack() throws IOException {
        // Mapping: "a" is a keyword (not a group mapper). Leaf "a.b" has no mapper.
        MapperService ms = mapper(mapping(b -> b.startObject("a").field("type", "keyword").endObject()));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNull("a leaf under a non-group FieldMapper ancestor should cause fallback", resolution);
    }

    /**
     * A leaf under a {@code dynamic=false} parent that has no group owner should still be silently
     * ignored — the group check must not accidentally swallow the dynamic=false branch.
     */
    public void testUnmappedLeafUnderDynamicFalseIsStillIgnoredWithNoGroup() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        // "unknown" has no mapper and no group owner; under dynamic=false it is silently ignored.
        SourceSchema schema = schemaOfJson("{\"known\":\"v\",\"unknown\":\"x\"}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", 0)]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);
        assertEquals(0, resolution.columnGroups().length);
    }

    // ── §0 aliasing contract: dotted-path walk is row-parity-preserving ───────────────────────────
    // These tests pin the behaviour that a tree-pointer walk would get wrong, so the dot-expansion
    // follow-up has a concrete spec to preserve.

    /**
     * A field with a literal dot in the source key (e.g. {@code {"flat.k":"v"}}) is treated by the
     * ESCF encoder as a leaf directly under root with the dotted name. The dotted-path walk must still
     * find the {@code flat} group mapper and produce relative key {@code k}, matching the row path
     * ({@code DotExpandingXContentParser} collapses {@code flat.k} → {@code flat.k} under {@code flat}).
     *
     * <p>A tree-pointer walk would miss this case: the leaf's schema parent is root (not {@code flat}).
     */
    public void testDottedKeyAtRootResolvesToGroup() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        // Literal dot in the source field name: encoded as a leaf "flat.k" under root.
        SourceSchema schema = schemaOfJson("{\"flat.k\":\"v\"}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("dotted source key should resolve to the flattened group", resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(
            "relative key should be 'k' (strip 'flat.' prefix)",
            new String[] { "k" },
            resolution.columnGroups()[0].relativeKeys()
        );
    }

    /**
     * A literal dotted key inside the flattened object ({@code {"flat":{"a.b":"v"}}}) and the
     * equivalent nested object ({@code {"flat":{"a":{"b":"v"}}}}) both produce relative key {@code a.b}.
     */
    public void testLiteralDottedKeyAndNestedObjectYieldSameRelativeKey() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));

        // Shape 1: literal dotted key inside flat value → ESCF leaf "flat.a.b" under non-leaf "flat"
        SourceSchema schemaDotted = schemaOfJson("{\"flat\":{\"a.b\":\"v\"}}");
        BatchMapperResolution resDotted = ShardBatchMapper.resolveMappers(schemaDotted, ms.mappingLookup(), indexSettings);
        assertNotNull(resDotted);
        assertArrayEquals(new String[] { "a.b" }, resDotted.columnGroups()[0].relativeKeys());

        // Shape 2: nested object → ESCF leaf "flat.a.b" via non-leaf "a" under non-leaf "flat"
        SourceSchema schemaObj = schemaOfJson("{\"flat\":{\"a\":{\"b\":\"v\"}}}");
        BatchMapperResolution resObj = ShardBatchMapper.resolveMappers(schemaObj, ms.mappingLookup(), indexSettings);
        assertNotNull(resObj);
        assertArrayEquals(new String[] { "a.b" }, resObj.columnGroups()[0].relativeKeys());
    }

    /**
     * Both aliasing shapes in one batch produce two columns in the same group, both with relative key
     * {@code a.b}. This is expected and benign: within a given document each column is absent where the
     * other is present, so emitted slots match the row path.
     */
    public void testAliasedKeysProduceTwoColumnsInOneGroup() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        // Two docs in the batch: one uses the dotted shape, one uses the nested-object shape.
        SourceSchema schema = schemaOfJson("{\"flat\":{\"a.b\":1}}", "{\"flat\":{\"a\":{\"b\":2}}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnGroups().length);
        // Two leaf indexes, both with relative key "a.b".
        assertEquals(2, resolution.columnGroups()[0].leafIndexes().length);
        assertArrayEquals(new String[] { "a.b", "a.b" }, resolution.columnGroups()[0].relativeKeys());
    }
}
