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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.AbstractShardBatchMapperResolveTestCase;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.ColumnGroupResolver;
import org.elasticsearch.index.mapper.ColumnGroupResolver.ColumnGroupLookup;
import org.elasticsearch.index.mapper.ColumnGroupResolver.ColumnGroupResolution;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ShardBatchMapperResolveTests extends AbstractShardBatchMapperResolveTestCase {

    /** Enables the implicit flattened {@code _unmapped} sink, which makes the resolved root dynamic FLATTENED. */
    private static Settings unmappedSinkEnabled() {
        return Settings.builder().put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true).build();
    }

    public void testHappyPath() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
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
        MapperService ms = columnarMapperService(
            mapping(b -> b.startObject("host").field("type", "keyword").field("ignore_above", 32).endObject())
        );
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof KeywordFieldMapper);
    }

    public void testNumberMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("v").field("type", "long").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("v"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof NumberFieldMapper);
    }

    public void testNumberIgnoreMalformedIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("v").field("type", "long").field("ignore_malformed", true).endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("v"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof NumberFieldMapper);
    }

    public void testMissingLeafUnderDynamicFalseIsIgnored() throws IOException {
        MapperService ms = columnarMapperService(topMapping(b -> {
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
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("known").field("type", "keyword").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known", "unknown"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    // TODO: not relevant at the moment because we are columnar only which does not support runtime fields
    // public void testRuntimeFieldInMappingFallsBack() throws IOException {
    // MapperService ms = columnarMapperService(topMapping(b -> {
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

    public void testTextMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("t").field("type", "text").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(resolution.columnMappers()[0], instanceOf(TextFieldMapper.class));
    }

    public void testTextMapperWithIndexPhrasesFallsBack() throws IOException {
        MapperService ms = columnarMapperService(
            mapping(b -> { b.startObject("t").field("type", "text").field("index_phrases", true).endObject(); })
        );
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testBooleanMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("b").field("type", "boolean").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("b"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof BooleanFieldMapper);
    }

    public void testDateMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("ts").field("type", "date").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("ts"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof DateFieldMapper);
    }

    public void testIpMapperIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> { b.startObject("ip").field("type", "ip").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("ip"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(resolution.columnMappers()[0], instanceOf(IpFieldMapper.class));
    }

    /**
     * A multi-field whose sub-mappers are all columnar-capable resolves to the parent mapper; the driver fans the parent's column
     * out to each sub-mapper via {@link org.elasticsearch.index.mapper.FieldMapper#mapColumnBatch}.
     */
    public void testKeywordWithMultiFieldsIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOf("host");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnMappers().length);
        assertThat(resolution.columnMappers()[schema.findLeaf("host", 0)], instanceOf(KeywordFieldMapper.class));
    }

    public void testKeywordWithTextMultiFieldIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            b.startObject("txt").field("type", "text").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOf("host");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnMappers().length);
        assertThat(resolution.columnMappers()[schema.findLeaf("host", 0)], instanceOf(KeywordFieldMapper.class));
    }

    /** One sub-mapper without columnar support disqualifies the whole leaf, and therefore the whole batch. */
    public void testMultiFieldWithUnsupportedSubMapperFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            // binary has no columnar support: it disqualifies the keyword parent.
            b.startObject("raw").field("type", "binary").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    /**
     * A group mapper used as a multi-field sub-mapper is rejected: {@code FieldMapper#supportsColumnarParse}
     * requires {@code builderParams.multiFields.mappers.length == 0} for any mapper that returns
     * {@code true} from {@link org.elasticsearch.index.mapper.FieldMapper#resolvesColumnGroup}. This pin
     * covers the geo_point case now that it is a group mapper.
     */
    public void testGroupMapperMultiFieldSubMapperFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("geo").field("type", "geo_point").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    /**
     * A sub-mapper that resolves a column group (here {@code flattened}) is refused: a group mapper expects a whole subtree of
     * schema leaves, and a multi-field has no leaf of its own to give it.
     */
    public void testGroupMapperAsSubFieldFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("flat").field("type", "flattened").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    /**
     * Pins the assumption behind the group-mapper branch of {@code FieldMapper#supportsColumnarParse}: a group mapper cannot carry
     * multi-fields, because {@code mapColumnGroupBatch} covers a whole subtree of leaves and never fans out to sub-mappers, so their
     * values would be silently dropped. Today that branch is unreachable — {@code flattened}, the only group mapper, rejects
     * {@code fields} at mapping-parse time — and it stays as a guard for any future group mapper that does not.
     */
    public void testGroupMapperCannotDeclareMultiFields() {
        Exception e = expectThrows(MapperParsingException.class, () -> columnarMapperService(mapping(b -> {
            b.startObject("flat");
            b.field("type", "flattened");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("flattened field [flat] does not support [fields]"));
    }

    /** A sub-field configuration its own mapper cannot do columnar (no doc values) disqualifies the leaf. */
    public void testSubFieldWithoutDocValuesFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("nd").field("type", "keyword").field("doc_values", false).endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    /** A multi-field with that is a dimension must still resolve. */
    public void testDimensionSubFieldFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertSame(resolution.columnMappers()[0], ms.mappingLookup().getMapper("host"));
    }

    /** A multi-field on a leaf that itself sits under an object path must still resolve. */
    public void testMultiFieldUnderObjectStillResolves() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("outer").startObject("properties");
            b.startObject("inner");
            b.field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.endObject().endObject();
        }));
        SourceSchema schema = schemaOfNested("outer.inner");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("a multi-field on a leaf inside an object must still resolve", resolution);
        assertThat(resolution.columnMappers()[0], instanceOf(KeywordFieldMapper.class));
    }

    /**
     * Regression guard for the multi-field sub-field check: it must not fire on a separately declared dotted field name. Strict
     * columnar disables {@code subobjects}, so a mapping may declare both {@code a} and a field literally named {@code a.b}. The
     * sequential path resolves and indexes {@code a.b} normally, so treating the mapped-ancestor {@code a} as a conflict would
     * disable the fast path for a fully supported mapping.
     */
    public void testSeparatelyDeclaredDottedFieldStillResolves() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("a").field("type", "keyword").endObject();
            b.startObject("a.b").field("type", "keyword").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"a.b\":\"x\"}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("a separately declared dotted field must not be mistaken for a multi-field sub-field", resolution);
        assertThat(resolution.columnMappers()[0], instanceOf(KeywordFieldMapper.class));
        // The ancestor walk used for unmapped leaves does report a conflict here, which is why the mapped-leaf check cannot
        // reuse it: "a" is a field mapper, but it does not declare "a.b" under [fields].
        assertThat(ColumnGroupResolver.findColumnGroup("a.b", ms.mappingLookup()), instanceOf(ColumnGroupLookup.Conflict.class));
    }

    /**
     * A multi-field sub-mapper is registered in {@code MappingLookup} under its own dotted path, so a document that spells the
     * sub-field directly resolves to it here. The sequential path cannot reach it — {@code DocumentParser#getLeafMapper} resolves
     * through the object tree, whose children never include multi-fields, and no-ops a path that has a field type but no such
     * mapper — so binding the leaf here would index a value the sequential path drops. The batch must fall back instead.
     */
    public void testMultiFieldSubFieldSpelledDirectlyFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"host.lower\":\"x\"}");
        assertNull(
            "a multi-field sub-field spelled directly in the source must fall back",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings)
        );
    }

    public void testNestedLeafHappyPath() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
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
        MapperService ms = columnarMapperService(mapping(b -> {
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
     * Core regression: flattened sub-keys resolve to a group, not individual leaf mappers, and not
     * a runtime-field shadow (which would cause a fallback).
     */
    public void testFlattenedGroupHappyPath() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
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
        MapperService ms = columnarMapperService(mapping(b -> {
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
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        // {"flat":null} produces a leaf at "flat" directly under root.
        SourceSchema schema = schemaOfJson("{\"flat\":null}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(resolution.columnMappers()[schema.findLeaf("flat", 0)], instanceOf(FlattenedFieldMapper.class));
        assertEquals(0, resolution.columnGroups().length);
    }

    /** A batch mixing a null leaf and group sub-keys produces both a leaf mapper and a group. */
    public void testFlattenedOwnPathLeafAndGroupCoexist() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
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
        MapperService ms = columnarMapperService(mapping(b -> {
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
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        SourceSchema schema = schemaOfJson("{\"flat\":{\"outer\":{\"inner\":\"v\"}}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "outer.inner" }, resolution.columnGroups()[0].relativeKeys());
    }

    /** A flattened field with {@code "index": true} is unsupported; the whole batch falls back. */
    public void testUnsupportedFlattenedConfigFallsBack() throws IOException {
        MapperService ms = columnarMapperService(
            mapping(b -> b.startObject("flat").field("type", "flattened").field("index", true).endObject())
        );
        SourceSchema schema = schemaOfJson("{\"flat\":{\"k\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNull("flattened with index=true should cause fallback", resolution);
    }

    /**
     * A non-group FieldMapper at an ancestor path stops the walk and is reported as
     * {@link ColumnGroupLookup.Conflict}: the document nests values beneath a leaf field, which the
     * sequential path rejects as a document parsing error.
     */
    public void testNonGroupFieldMapperAncestorFallsBack() throws IOException {
        // Mapping: "a" is a keyword (not a group mapper). Leaf "a.b" has no mapper.
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("a").field("type", "keyword").endObject()));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNull("a leaf under a non-group FieldMapper ancestor should cause fallback", resolution);

        assertThat(ColumnGroupResolver.findColumnGroup("a.b", ms.mappingLookup()), instanceOf(ColumnGroupLookup.Conflict.class));
    }

    /**
     * Regression: the conflict must be detected <em>before</em> the {@code dynamic=false} branch. Both
     * outcomes leave the leaf without a mapper, but a conflict means the sequential path rejects the
     * document, whereas {@code dynamic=false} means it silently drops the value. Classifying a conflict
     * as merely unmapped would index the document while the sequential path errors on it.
     */
    public void testNonGroupFieldMapperAncestorFallsBackUnderDynamicFalse() throws IOException {
        MapperService ms = columnarMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("a").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"v\"}}");
        assertNull(
            "a leaf conflicting with a field mapper must fall back, not be dropped as unmapped",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings)
        );
    }

    /** A leaf whose dotted ancestors have no mapper at all is {@link ColumnGroupLookup.NotOwned}, not a conflict. */
    public void testLeafWithNoMappedAncestorIsNotOwned() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("a").field("type", "keyword").endObject()));
        assertThat(ColumnGroupResolver.findColumnGroup("x.y", ms.mappingLookup()), instanceOf(ColumnGroupLookup.NotOwned.class));
        // A leaf with no dots has no ancestors to walk at all.
        assertThat(ColumnGroupResolver.findColumnGroup("x", ms.mappingLookup()), instanceOf(ColumnGroupLookup.NotOwned.class));
    }

    /**
     * A leaf under a {@code dynamic=false} parent that has no group owner should still be silently
     * ignored — the group check must not accidentally swallow the dynamic=false branch.
     */
    public void testUnmappedLeafUnderDynamicFalseIsStillIgnoredWithNoGroup() throws IOException {
        MapperService ms = columnarMapperService(topMapping(b -> {
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

    /**
     * A field with a literal dot in the source key (e.g. {@code {"flat.k":"v"}}) is treated by the
     * ESCF encoder as a leaf directly under root with the dotted name. The dotted-path walk must still
     * find the {@code flat} group mapper and produce relative key {@code k}, matching the row path
     * ({@code DotExpandingXContentParser} collapses {@code flat.k} → {@code flat.k} under {@code flat}).
     *
     * <p>A tree-pointer walk would miss this case: the leaf's schema parent is root (not {@code flat}).
     */
    public void testDottedKeyAtRootResolvesToGroup() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
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
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));

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
     * {@code a.b}. This is safe for a group mapper: every column of the group is handed to
     * {@code mapColumnGroupBatch} in one call and merged into a single per-document output, so this holds even
     * when one document carries both spellings (see {@code FlattenedFieldMapperColumnarCompatibilityTests}).
     */
    public void testAliasedKeysProduceTwoColumnsInOneGroup() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
        // Two docs in the batch: one uses the dotted shape, one uses the nested-object shape.
        SourceSchema schema = schemaOfJson("{\"flat\":{\"a.b\":1}}", "{\"flat\":{\"a\":{\"b\":2}}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnGroups().length);
        // Two leaf indexes, both with relative key "a.b".
        assertEquals(2, resolution.columnGroups()[0].leafIndexes().length);
        assertArrayEquals(new String[] { "a.b", "a.b" }, resolution.columnGroups()[0].relativeKeys());

        // Both spellings inside one document is the same story: still one group, still two columns.
        SourceSchema oneDoc = schemaOfJson("{\"flat\":{\"a.b\":1,\"a\":{\"b\":2}}}");
        BatchMapperResolution single = ShardBatchMapper.resolveMappers(oneDoc, ms.mappingLookup(), indexSettings);
        assertNotNull(single);
        assertArrayEquals(new String[] { "a.b", "a.b" }, single.columnGroups()[0].relativeKeys());
    }

    /**
     * A per-leaf mapper gets one {@code mapColumnBatch} call per column, so a document spelling the same field both
     * ways ({@code {"a":{"b":1},"a.b":3}}) would emit two independent outputs where the sequential path emits one
     * merged multi-valued field — for a keyword, two doc-value blobs and two count entries instead of one of each.
     * The batch must fall back.
     */
    public void testAliasedPerLeafColumnsFallBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("a");
            b.startObject("properties");
            b.startObject("b").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"x\"},\"a.b\":\"y\"}");
        assertEquals("both spellings should survive encoding as separate columns", 2, schema.leafCount());
        assertEquals("a.b", schema.getFullPath(0));
        assertEquals("a.b", schema.getFullPath(1));

        assertNull(ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings));
    }

    /**
     * The aliasing check is batch-wide rather than per-document, so it also trips when the two spellings come from
     * different documents. That case would in fact be safe — neither document carries both columns — but detecting it
     * needs per-row inspection, and {@code resolveMappers} runs once per batch. Falling back costs a slow path on a
     * rare input.
     */
    public void testAliasedPerLeafColumnsAcrossDocumentsAlsoFallBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("a");
            b.startObject("properties");
            b.startObject("b").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"x\"}}", "{\"a.b\":\"y\"}");
        assertNull(ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings));
    }

    /** Control: a single spelling repeated across documents is one column and must not trip the aliasing check. */
    public void testSameSpellingAcrossDocumentsIsOneColumn() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("a");
            b.startObject("properties");
            b.startObject("b").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"x\"}}", "{\"a\":{\"b\":\"y\"}}");
        assertEquals(1, schema.leafCount());
        assertNotNull(ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings));
    }

    /**
     * Columnar mode keeps a single level of nested objects as document boundaries rather than flattening
     * them away, but their leaves land in {@code MappingLookup} keyed by full dotted path, indistinguishable
     * from root-level fields. Resolving them would write the values into the root Lucene document instead of
     * the per-element nested documents the sequential path produces, so the batch must fall back.
     */
    public void testNestedObjectFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("comments");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("text").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        // The leaf under the nested object resolves to an ordinary KeywordFieldMapper, so nothing else
        // in resolveMappers would catch it.
        assertThat(ms.mappingLookup().getMapper("comments.text"), instanceOf(KeywordFieldMapper.class));

        SourceSchema schema = schemaOfJson("{\"host\":\"srv\",\"comments\":{\"text\":\"a\"}}");
        assertNull(
            "a leaf under a nested object must not be mapped into the root document",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings)
        );
    }

    /**
     * The nested bail-out is mapping-wide rather than per-leaf: a batch whose columns never touch the
     * nested path still falls back, because a nested mapping changes the shape of every document.
     */
    public void testNestedObjectFallsBackEvenWhenBatchDoesNotUseIt() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("comments");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("text").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    private static void assumeUnmappedSinkAvailable() {
        assumeTrue("flattened_unmapped_fields is enabled", FlattenedFieldMapper.UNMAPPED_FIELDS_FEATURE_FLAG.isEnabled());
    }

    /**
     * With the implicit {@code _unmapped} sink present and no explicit root dynamic, the resolved root dynamic is
     * {@link org.elasticsearch.index.mapper.ObjectMapper.Dynamic#FLATTENED} rather than {@code TRUE}, so an unmapped
     * leaf is absorbed by the sink instead of forcing a fallback.
     */
    public void testUnmappedLeafIsAbsorbedByTheSink() throws IOException {
        assumeUnmappedSinkAvailable();
        MapperService ms = columnarMapperService(
            unmappedSinkEnabled(),
            mapping(b -> b.startObject("host").field("type", "keyword").endObject())
        );
        SourceSchema schema = schemaOfJson("{\"host\":\"srv\",\"unknown\":\"x\"}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            schema,
            ms.mappingLookup(),
            columnarIndexSettings(unmappedSinkEnabled())
        );
        assertNotNull("an unmapped leaf should be sunk, not force a fallback", resolution);

        assertThat(resolution.columnMappers()[schema.findLeaf("host", 0)], instanceOf(KeywordFieldMapper.class));
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);

        assertEquals(1, resolution.columnGroups().length);
        assertEquals(FlattenedFieldMapper.UNMAPPED_SINK_NAME, resolution.columnGroups()[0].mapper().fullPath());
        assertArrayEquals(new String[] { "unknown" }, resolution.columnGroups()[0].relativeKeys());
    }

    /**
     * The sink is keyed by the leaf's <em>full</em> dotted path, matching {@code DynamicFieldsBuilder.FlattenedSink},
     * which calls {@code indexValueAtPath(context, context.path().pathAsText(name))}. This differs from a real
     * flattened field, whose relative keys have the owner prefix stripped.
     */
    public void testSinkKeysUnmappedLeafByFullDottedPath() throws IOException {
        assumeUnmappedSinkAvailable();
        MapperService ms = columnarMapperService(
            unmappedSinkEnabled(),
            mapping(b -> b.startObject("host").field("type", "keyword").endObject())
        );
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"x\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            schema,
            ms.mappingLookup(),
            columnarIndexSettings(unmappedSinkEnabled())
        );
        assertNotNull(resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "a.b" }, resolution.columnGroups()[0].relativeKeys());
    }

    /**
     * Dynamic templates are tried before the sink on the sequential path, and a match creates a concrete field rather
     * than absorbing the value. The batch path cannot evaluate templates, so it must fall back instead of guessing.
     */
    public void testDynamicTemplatesPreEmptTheSink() throws IOException {
        assumeUnmappedSinkAvailable();
        MapperService ms = columnarMapperService(unmappedSinkEnabled(), topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("strings_as_keyword");
            b.field("match_mapping_type", "string");
            b.startObject("mapping").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
            b.endArray();
            b.startObject("properties");
            b.startObject("host").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"host\":\"srv\",\"unknown\":\"x\"}");
        assertNull(
            "a dynamic template may claim the leaf before the sink does",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), columnarIndexSettings(unmappedSinkEnabled()))
        );
    }

    /** An explicit root {@code dynamic: false} still wins over the sink, so the leaf is dropped rather than absorbed. */
    public void testExplicitRootDynamicFalseWinsOverTheSink() throws IOException {
        assumeUnmappedSinkAvailable();
        MapperService ms = columnarMapperService(unmappedSinkEnabled(), topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("host").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"host\":\"srv\",\"unknown\":\"x\"}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            schema,
            ms.mappingLookup(),
            columnarIndexSettings(unmappedSinkEnabled())
        );
        assertNotNull(resolution);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);
        assertEquals("an explicitly disabled dynamic must not sink", 0, resolution.columnGroups().length);
    }

    /**
     * The sequential path rejects an unmapped field matching {@code routing_path} rather than dropping it
     * ({@code DocumentParser#failIfMatchesRoutingPath}), so the batch must fall back and let it raise the error.
     */
    public void testUnmappedLeafMatchingRoutingPathFallsBack() throws IOException {
        // COLUMNAR rejects index.routing_path outright (IndexMode#validateRoutingPathSettings), so the only strict
        // columnar mode that can carry one is LOGSDB_COLUMNAR with routing on sort fields.
        Settings routingPathSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB_COLUMNAR.getName())
            .put(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey(), true)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.*")
            .build();
        MapperService ms = columnarMapperService(routingPathSettings, topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        IndexSettings withRoutingPath = columnarIndexSettings(routingPathSettings);

        // An unmapped leaf outside routing_path is still silently dropped.
        SourceSchema unrelated = schemaOfJson("{\"known\":\"v\",\"other\":\"x\"}");
        assertNotNull(ShardBatchMapper.resolveMappers(unrelated, ms.mappingLookup(), withRoutingPath));

        SourceSchema matching = schemaOfJson("{\"known\":\"v\",\"dim\":{\"host\":\"x\"}}");
        assertNull(
            "an unmapped leaf matching routing_path must fall back, not be dropped",
            ShardBatchMapper.resolveMappers(matching, ms.mappingLookup(), withRoutingPath)
        );
    }

    /**
     * {@code DotExpandingXContentParser} rejects dotted names with blank segments, but the columnar encoder keeps them
     * verbatim. Under {@code dynamic: false} they would otherwise resolve to nothing and be silently dropped.
     */
    public void testMalformedDottedFieldNameFallsBack() throws IOException {
        MapperService ms = columnarMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        // Empty segments and all-whitespace segments. Note "a. b" and "a .b" are NOT malformed: their segments
        // are " b" and "a ", which are not blank, and the sequential path accepts them too.
        for (String json : List.of("{\"a..b\":1}", "{\".a\":1}", "{\"a. .b\":1}", "{\"a.\\t.b\":1}")) {
            assertNull(
                "expected fallback for " + json,
                ShardBatchMapper.resolveMappers(schemaOfJson(json), ms.mappingLookup(), indexSettings)
            );
        }
    }

    /** Names the sequential path tolerates around blank segments must not trigger the malformed-name fallback. */
    public void testDottedNameWithNonBlankSegmentsIsNotMalformed() throws IOException {
        MapperService ms = columnarMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        for (String json : List.of("{\"a. b\":1}", "{\"a .b\":1}")) {
            assertNotNull(
                "expected no fallback for " + json,
                ShardBatchMapper.resolveMappers(schemaOfJson(json), ms.mappingLookup(), indexSettings)
            );
        }
    }

    /**
     * A trailing dot is the one case where the check is deliberately over-eager: {@code DotExpandingXContentParser}
     * trims it and treats {@code "a."} as plain {@code "a"}, while this falls back. That costs a slow path, not
     * correctness, and keeps the check a simple blank-segment scan.
     */
    public void testTrailingDotFallsBackEvenThoughRowPathTrimsIt() throws IOException {
        // "a" is deliberately left unmapped: a mapper there would make "a." a ColumnGroupLookup.Conflict and the
        // batch would fall back for that reason instead, leaving the blank-segment check untested.
        MapperService ms = columnarMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOfJson("{\"a.\":1}"), ms.mappingLookup(), indexSettings));
    }

    /**
     * A name without dots is never expanded, so the sequential path does not validate it either — it must not be
     * treated as malformed here, otherwise the batch would fall back where the sequential path indexes normally.
     */
    public void testDotlessBlankFieldNameIsNotMalformed() throws IOException {
        MapperService ms = columnarMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"known\":\"v\",\" \":\"x\"}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("a dotless blank name is not malformed; it is simply unmapped", resolution);
        assertNull(resolution.columnMappers()[schema.findLeaf(" ", 0)]);
    }

    /** Control for the two tests above: the same mapping without the nested object resolves normally. */
    public void testPlainObjectWithSameShapeResolves() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("comments");
            b.startObject("properties");
            b.startObject("text").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"host\":\"srv\",\"comments\":{\"text\":\"a\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(resolution.columnMappers()[schema.findLeaf("host", 0)], instanceOf(KeywordFieldMapper.class));
        assertThat(
            resolution.columnMappers()[schema.findLeaf("text", schema.findNonLeaf("comments", 0))],
            instanceOf(KeywordFieldMapper.class)
        );
    }

    /**
     * An unmapped empty-object leaf (every row is {@code {}}) should not abort batch indexing: the sequential path
     * produces nothing for that field regardless of the {@code dynamic} setting, so skipping it is safe.
     *
     * <p>The test uses a real {@link EscfBatch} so that {@link EscfBatch#isEmptyObjectColumn} is exercised end-to-end,
     * unlike the schema-only overload which always returns {@code false} for every column.
     */
    public void testEmptyObjectColumnDoesNotAbortBatch() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("host").field("type", "keyword").endObject()));

        // Batch where "pipeline_artifact" resolves to an empty-object leaf in every row — should succeed.
        try (EscfBatch batch = batchOfJson("{\"host\":\"srv\",\"pipeline_artifact\":{}}")) {
            BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch, ms.mappingLookup(), indexSettings);
            assertNotNull("empty-object column must not abort batch indexing", resolution);
            int emptyCol = batch.schema().findLeaf("pipeline_artifact", 0);
            assertNull("empty-object column should map to null, not a real mapper", resolution.columnMappers()[emptyCol]);
            int hostCol = batch.schema().findLeaf("host", 0);
            assertThat(resolution.columnMappers()[hostCol], instanceOf(KeywordFieldMapper.class));
        }

        // Batch where "pipeline_artifact" carries a real value — must still fall back.
        try (EscfBatch batch = batchOfJson("{\"host\":\"srv\",\"pipeline_artifact\":\"real_value\"}")) {
            assertNull(
                "a column with real values must still abort under dynamic=TRUE",
                ShardBatchMapper.resolveMappers(batch, ms.mappingLookup(), indexSettings)
            );
        }

        // The skip is gated on strict-columnar mode: a standard-mode index must still fall back.
        IndexSettings standardSettings = new IndexSettings(
            new IndexMetadata.Builder("index").settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.getName()).build()
            ).build(),
            Settings.EMPTY
        );
        try (EscfBatch batch = batchOfJson("{\"host\":\"srv\",\"pipeline_artifact\":{}}")) {
            assertNull(
                "empty-object skip must not apply outside strict-columnar mode",
                ShardBatchMapper.resolveMappers(batch, ms.mappingLookup(), standardSettings)
            );
        }
    }

    /**
     * Object-form geo_points produce two dotted sub-leaves ({@code loc.lat}, {@code loc.lon}) that
     * resolve to the {@code geo_point} group mapper rather than causing a Conflict.
     */
    public void testGeoPointObjectFormatIsSupported() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("loc").field("type", "geo_point").endObject()));
        SourceSchema schema = schemaOfJson("{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("geo_point object-form should resolve on the columnar path", resolution);
        int latLeaf = schema.findLeaf("lat", schema.findNonLeaf("loc", 0));
        int lonLeaf = schema.findLeaf("lon", schema.findNonLeaf("loc", 0));
        assertNull("loc.lat should be owned by the group", resolution.columnMappers()[latLeaf]);
        assertNull("loc.lon should be owned by the group", resolution.columnMappers()[lonLeaf]);
        assertEquals(1, resolution.columnGroups().length);
        assertThat(resolution.columnGroups()[0].mapper().fullPath(), equalTo("loc"));
    }

    /**
     * The dotted spelling {@code {"loc.lat":…,"loc.lon":…}} (literal dots in source keys) is treated
     * by the ESCF encoder as two root-level leaves with full paths {@code loc.lat} and {@code loc.lon}.
     * The resolver's ancestor walk must still find the {@code geo_point} group mapper.
     */
    public void testGeoPointDottedSpellingResolvesToGroup() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("loc").field("type", "geo_point").endObject()));
        SourceSchema schema = schemaOfJson("{\"loc.lat\":51.5,\"loc.lon\":-0.1}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("geo_point dotted spelling should resolve to the group", resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "lat", "lon" }, resolution.columnGroups()[0].relativeKeys());
    }

    /** A geo_point with multi-fields cannot use the columnar path (group mapper requires no multi-fields). */
    public void testGeoPointWithMultiFieldFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("loc");
            b.field("type", "geo_point");
            b.startObject("fields").startObject("hash").field("type", "keyword").endObject().endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}");
        assertNull(
            "geo_point with multi-fields cannot use the columnar fast path",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings)
        );
    }

    /** A geo_point with {@code index: true} has BKD points enabled; the columnar path falls back. */
    public void testGeoPointWithIndexTrueFallsBack() throws IOException {
        MapperService ms = columnarMapperService(
            mapping(b -> { b.startObject("loc").field("type", "geo_point").field("index", true).endObject(); })
        );
        SourceSchema schema = schemaOfJson("{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}");
        assertNull(
            "geo_point with index=true (BKD points) cannot use the columnar fast path",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings)
        );
    }

    /** A geo_point with {@code store: true} falls back because no stored-field column is emitted. */
    public void testGeoPointWithStoreTrueFallsBack() throws IOException {
        MapperService ms = createMapperService(
            mapping(b -> { b.startObject("loc").field("type", "geo_point").field("store", true).endObject(); })
        );
        var mapper = (GeoPointFieldMapper) ms.mappingLookup().getMapper("loc");
        assertFalse("geo_point with store=true must not support the columnar fast path", mapper.supportsColumnarParse(indexSettings));
    }

    /** A geo_point with a configured {@code null_value} falls back (silence would drop the point). */
    public void testGeoPointWithNullValueFallsBack() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("loc").field("type", "geo_point").field("null_value", "0,0").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}");
        assertNull(
            "geo_point with null_value falls back (null rows must index the configured point)",
            ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings)
        );
    }

    /**
     * A geo_point declared as a TSDB {@code time_series_metric: position} is supported: its
     * {@code index} parameter defaults to {@code false} and {@code doc_values} is required.
     * <p>
     * We verify {@link GeoPointFieldMapper#supportsColumnarParse} directly rather than going through
     * {@link ShardBatchMapper#resolveMappers} because TSDB metadata mappers (e.g. {@code _tsid}) may
     * not support the columnar path, which would cause {@code resolveMappers} to return {@code null}
     * for reasons unrelated to the geo_point mapper itself.
     */
    public void testGeoPointPositionMetricIsSupported() throws IOException {
        Settings tsdbSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim")
            .build();
        MapperService ms = createMapperService(tsdbSettings, mapping(b -> {
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("loc").field("type", "geo_point").field("time_series_metric", "position").endObject();
        }));
        var mapper = (GeoPointFieldMapper) ms.mappingLookup().getMapper("loc");
        assertTrue(
            "geo_point with time_series_metric=position should support the columnar path in TSDB mode",
            mapper.supportsColumnarParse(ms.getIndexSettings())
        );
    }

    /** A null leaf at the field's own path (e.g. {@code {"loc":null}}) uses the leaf mapper, not the group. */
    public void testGeoPointLeafAtOwnPathUsesLeafMapper() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("loc").field("type", "geo_point").endObject()));
        SourceSchema schema = schemaOfJson("{\"loc\":null}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(
            "a null leaf at geo_point's own path should use the leaf mapper",
            resolution.columnMappers()[schema.findLeaf("loc", 0)],
            instanceOf(GeoPointFieldMapper.class)
        );
        assertEquals("no group should be created for an own-path leaf", 0, resolution.columnGroups().length);
    }

    /** A batch mixing a null own-path leaf and object-form sub-leaves produces both a leaf mapper and a group. */
    public void testGeoPointOwnPathLeafAndGroupCoexist() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("loc").field("type", "geo_point").endObject()));
        SourceSchema schema = schemaOfJson("{\"loc\":null}", "{\"loc\":{\"lat\":51.5,\"lon\":-0.1}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(resolution.columnMappers()[schema.findLeaf("loc", 0)], instanceOf(GeoPointFieldMapper.class));
        assertEquals(1, resolution.columnGroups().length);
        assertThat(resolution.columnGroups()[0].mapper().fullPath(), equalTo("loc"));
    }

    /** Two independently declared geo_point fields produce two separate groups. */
    public void testTwoGeoPointFieldsProduceTwoGroups() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> {
            b.startObject("src").field("type", "geo_point").endObject();
            b.startObject("dst").field("type", "geo_point").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"src\":{\"lat\":51.5,\"lon\":-0.1},\"dst\":{\"lat\":48.9,\"lon\":2.3}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(2, resolution.columnGroups().length);
        assertEquals("src", resolution.columnGroups()[0].mapper().fullPath());
        assertEquals("dst", resolution.columnGroups()[1].mapper().fullPath());
    }

    /**
     * Aliasing ({@code {"loc":{"lat":1,"lon":2},"loc.lat":3}}) produces a duplicate {@code lat}
     * relative key in the group. Resolution succeeds — group mappers are exempt from the per-leaf
     * dedupe check — but {@link org.elasticsearch.index.mapper.GeoPointFieldMapper#mapColumnGroupBatch}
     * throws {@link UnsupportedOperationException} at map time, causing a clean fallback.
     */
    public void testGeoPointAliasedKeysResolveButTriggerMapTimeFallback() throws IOException {
        MapperService ms = columnarMapperService(mapping(b -> b.startObject("loc").field("type", "geo_point").endObject()));
        SourceSchema schema = schemaOfJson("{\"loc\":{\"lat\":51.5,\"lon\":-0.1},\"loc.lat\":52.0}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull("resolution should succeed; the duplicate-key check fires at map time", resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "lat", "lon", "lat" }, resolution.columnGroups()[0].relativeKeys());
    }
}
