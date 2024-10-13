/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis.index;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.FieldCapsUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.sql.type.SqlDataTypeRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.isDateTime;
import static org.elasticsearch.xpack.ql.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.sql.types.SqlTypesTests.loadMapping;

public class IndexResolverTests extends ESTestCase {

    public void testMergeSameMapping() throws Exception {
        Map<String, EsField> oneMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> sameMapping = loadMapping("mapping-basic.json", true);
        assertNotSame(oneMapping, sameMapping);
        assertEquals(oneMapping, sameMapping);

        IndexResolution resolution = merge(new EsIndex("a", oneMapping), new EsIndex("b", sameMapping));

        assertTrue(resolution.isValid());
        assertEqualsMaps(oneMapping, resolution.get().mapping());
        assertEquals(Set.of("a", "b"), resolution.get().concreteIndices());
    }

    public void testMergeCompatibleMapping() throws Exception {
        Map<String, EsField> basicMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> numericMapping = loadMapping("mapping-numeric.json", true);

        assertNotEquals(basicMapping, numericMapping);

        IndexResolution resolution = merge(new EsIndex("basic", basicMapping), new EsIndex("numeric", numericMapping));

        assertTrue(resolution.isValid());
        assertEquals(basicMapping.size() + numericMapping.size(), resolution.get().mapping().size());
        assertEquals(Set.of("basic", "numeric"), resolution.get().concreteIndices());
    }

    public void testMergeIncompatibleTypes() throws Exception {
        Map<String, EsField> basicMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> incompatible = loadMapping("mapping-basic-incompatible.json");

        assertNotEquals(basicMapping, incompatible);

        String wildcard = "*";
        IndexResolution resolution = merge(new EsIndex("basic", basicMapping), new EsIndex("incompatible", incompatible));

        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        assertEquals(Set.of("basic", "incompatible"), esIndex.concreteIndices());
        EsField esField = esIndex.mapping().get("gender");
        assertEquals(InvalidMappedField.class, esField.getClass());

        assertEquals(
            "mapped as [2] incompatible types: [text] in [incompatible], [keyword] in [basic]",
            ((InvalidMappedField) esField).errorMessage()
        );
    }

    public void testMergeIncompatibleCapabilities() throws Exception {
        Map<String, EsField> basicMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> incompatible = loadMapping("mapping-basic-nodocvalues.json", true);

        assertNotEquals(basicMapping, incompatible);

        String wildcard = "*";
        IndexResolution resolution = merge(new EsIndex("basic", basicMapping), new EsIndex("incompatible", incompatible));

        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        EsField esField = esIndex.mapping().get("emp_no");
        assertEquals(InvalidMappedField.class, esField.getClass());
        assertEquals("mapped as aggregatable except in [incompatible]", ((InvalidMappedField) esField).errorMessage());
        assertEquals(Set.of("basic", "incompatible"), resolution.get().concreteIndices());
    }

    public void testMultiLevelObjectMappings() throws Exception {
        Map<String, EsField> dottedMapping = loadMapping("mapping-dotted-field.json", true);

        IndexResolution resolution = merge(new EsIndex("a", dottedMapping));

        assertTrue(resolution.isValid());
        assertEqualsMaps(dottedMapping, resolution.get().mapping());
        assertEquals(Set.of("a"), resolution.get().concreteIndices());
    }

    public void testMultiLevelNestedMappings() throws Exception {
        Map<String, EsField> nestedMapping = loadMapping("mapping-nested.json", true);

        IndexResolution resolution = merge(new EsIndex("a", nestedMapping));

        assertTrue(resolution.isValid());
        assertEqualsMaps(nestedMapping, resolution.get().mapping());
    }

    public void testMetaFieldsAreIgnored() throws Exception {
        Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();
        addFieldCaps(fieldCaps, "_version", "_version", true, false, false);
        addFieldCaps(fieldCaps, "_not_meta_field", "integer", false, true, true);
        addFieldCaps(fieldCaps, "_size", "integer", true, true, true);
        addFieldCaps(fieldCaps, "_doc_count", "long", true, false, false);
        addFieldCaps(fieldCaps, "text", "keyword", true, true);

        String wildcard = "*";
        IndexResolution resolution = mergedMappings(wildcard, new String[] { "org/elasticsearch/xpack/sql/index" }, fieldCaps);
        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        assertNull(esIndex.mapping().get("_version"));
        assertNull(esIndex.mapping().get("_size"));
        assertNull(esIndex.mapping().get("_doc_count"));
        assertEquals(INTEGER, esIndex.mapping().get("_not_meta_field").getDataType());
        assertEquals(KEYWORD, esIndex.mapping().get("text").getDataType());
        assertEquals(Set.of("org/elasticsearch/xpack/sql/index"), resolution.get().concreteIndices());
    }

    public void testFlattenedHiddenSubfield() throws Exception {
        Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();
        addFieldCaps(fieldCaps, "some_field", "flattened", false, false);
        addFieldCaps(fieldCaps, "some_field._keyed", "flattened", false, false);
        addFieldCaps(fieldCaps, "another_field", "object", true, false);
        addFieldCaps(fieldCaps, "another_field._keyed", "keyword", true, false);
        addFieldCaps(fieldCaps, "nested_field", "object", false, false);
        addFieldCaps(fieldCaps, "nested_field.sub_field", "flattened", true, true);
        addFieldCaps(fieldCaps, "nested_field.sub_field._keyed", "flattened", true, true);
        addFieldCaps(fieldCaps, "text", "keyword", true, true);

        String wildcard = "*";
        IndexResolution resolution = mergedMappings(wildcard, new String[] { "org/elasticsearch/xpack/sql/index" }, fieldCaps);
        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        assertEquals(Set.of("org/elasticsearch/xpack/sql/index"), resolution.get().concreteIndices());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("some_field").getDataType());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("some_field").getProperties().get("_keyed").getDataType());
        assertEquals(OBJECT, esIndex.mapping().get("nested_field").getDataType());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("nested_field").getProperties().get("sub_field").getDataType());
        assertEquals(
            UNSUPPORTED,
            esIndex.mapping().get("nested_field").getProperties().get("sub_field").getProperties().get("_keyed").getDataType()
        );
        assertEquals(KEYWORD, esIndex.mapping().get("text").getDataType());
        assertEquals(OBJECT, esIndex.mapping().get("another_field").getDataType());
        assertEquals(KEYWORD, esIndex.mapping().get("another_field").getProperties().get("_keyed").getDataType());
    }

    public void testPropagateUnsupportedTypeToSubFields() throws Exception {
        // generate a field type having the name of the format "foobar43"
        String esFieldType = randomAlphaOfLengthBetween(5, 10) + randomIntBetween(-100, 100);
        Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();
        addFieldCaps(fieldCaps, "a", "text", false, false);
        addFieldCaps(fieldCaps, "a.b", esFieldType, false, false);
        addFieldCaps(fieldCaps, "a.b.c", "object", true, false);
        addFieldCaps(fieldCaps, "a.b.c.d", "keyword", true, false);
        addFieldCaps(fieldCaps, "a.b.c.e", "foo", true, true);

        String wildcard = "*";
        IndexResolution resolution = mergedMappings(wildcard, new String[] { "org/elasticsearch/xpack/sql/index" }, fieldCaps);
        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        assertEquals(Set.of("org/elasticsearch/xpack/sql/index"), resolution.get().concreteIndices());
        assertEquals(TEXT, esIndex.mapping().get("a").getDataType());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("a").getProperties().get("b").getDataType());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("a").getProperties().get("b").getProperties().get("c").getDataType());
        assertEquals(
            UNSUPPORTED,
            esIndex.mapping().get("a").getProperties().get("b").getProperties().get("c").getProperties().get("d").getDataType()
        );
        assertEquals(
            UNSUPPORTED,
            esIndex.mapping().get("a").getProperties().get("b").getProperties().get("c").getProperties().get("e").getDataType()
        );
    }

    public void testRandomMappingFieldTypeMappedAsUnsupported() throws Exception {
        // generate a field type having the name of the format "foobar43"
        String esFieldType = randomAlphaOfLengthBetween(5, 10) + randomIntBetween(-100, 100);

        Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();
        addFieldCaps(fieldCaps, "some_field", esFieldType, false, false);
        addFieldCaps(fieldCaps, "another_field", "object", true, false);
        addFieldCaps(fieldCaps, "another_field._foo", esFieldType, true, false);
        addFieldCaps(fieldCaps, "nested_field", "object", false, false);
        addFieldCaps(fieldCaps, "nested_field.sub_field1", esFieldType, true, true);
        addFieldCaps(fieldCaps, "nested_field.sub_field1.bar", esFieldType, true, true);
        addFieldCaps(fieldCaps, "nested_field.sub_field2", esFieldType, true, true);
        // even if this is of a supported type, because it belongs to an UNSUPPORTED type parent, it should also be UNSUPPORTED
        addFieldCaps(fieldCaps, "nested_field.sub_field2.bar", "keyword", true, true);
        addFieldCaps(fieldCaps, "text", "keyword", true, true);

        String wildcard = "*";
        IndexResolution resolution = mergedMappings(wildcard, new String[] { "org/elasticsearch/xpack/sql/index" }, fieldCaps);
        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        assertEquals(Set.of("org/elasticsearch/xpack/sql/index"), resolution.get().concreteIndices());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("some_field").getDataType());
        assertEquals(OBJECT, esIndex.mapping().get("nested_field").getDataType());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("nested_field").getProperties().get("sub_field1").getDataType());
        assertEquals(
            UNSUPPORTED,
            esIndex.mapping().get("nested_field").getProperties().get("sub_field1").getProperties().get("bar").getDataType()
        );
        assertEquals(UNSUPPORTED, esIndex.mapping().get("nested_field").getProperties().get("sub_field2").getDataType());
        assertEquals(
            UNSUPPORTED,
            esIndex.mapping().get("nested_field").getProperties().get("sub_field2").getProperties().get("bar").getDataType()
        );
        assertEquals(KEYWORD, esIndex.mapping().get("text").getDataType());
        assertEquals(OBJECT, esIndex.mapping().get("another_field").getDataType());
        assertEquals(UNSUPPORTED, esIndex.mapping().get("another_field").getProperties().get("_foo").getDataType());
    }

    public void testMergeIncompatibleCapabilitiesOfObjectFields() throws Exception {
        Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();

        int depth = randomInt(5);

        List<String> level = new ArrayList<>();
        String fieldName = randomAlphaOfLength(3);
        level.add(fieldName);
        for (int i = 0; i <= depth; i++) {
            String l = randomAlphaOfLength(3);
            level.add(l);
            fieldName += "." + l;
        }

        // define a sub-field
        addFieldCaps(fieldCaps, fieldName + ".keyword", "keyword", true, true);

        Map<String, FieldCapabilities> multi = new HashMap<>();
        multi.put(
            "long",
            new FieldCapabilities(fieldName, "long", false, true, true, new String[] { "one-index" }, null, null, Collections.emptyMap())
        );
        multi.put(
            "text",
            new FieldCapabilities(
                fieldName,
                "text",
                false,
                true,
                false,
                new String[] { "another-index" },
                null,
                null,
                Collections.emptyMap()
            )
        );
        fieldCaps.put(fieldName, multi);

        String wildcard = "*";
        IndexResolution resolution = mergedMappings(wildcard, new String[] { "one-index" }, fieldCaps);

        assertTrue(resolution.isValid());

        EsIndex esIndex = resolution.get();
        assertEquals(wildcard, esIndex.name());
        assertEquals(Set.of("one-index"), resolution.get().concreteIndices());
        EsField esField = null;
        Map<String, EsField> props = esIndex.mapping();
        for (String lvl : level) {
            esField = props.get(lvl);
            props = esField.getProperties();
        }
        assertEquals(InvalidMappedField.class, esField.getClass());
        assertEquals(
            "mapped as [2] incompatible types: [text] in [another-index], [long] in [one-index]",
            ((InvalidMappedField) esField).errorMessage()
        );
    }

    public void testSeparateSameMappingDifferentIndices() throws Exception {
        Map<String, EsField> oneMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> sameMapping = loadMapping("mapping-basic.json", true);
        assertNotSame(oneMapping, sameMapping);
        assertEquals(oneMapping, sameMapping);

        List<EsIndex> indices = separate(new EsIndex("a", oneMapping), new EsIndex("b", sameMapping));

        assertEquals(2, indices.size());
        assertEqualsMaps(oneMapping, indices.get(0).mapping());
        assertEquals(Set.of("a"), indices.get(0).concreteIndices());
        assertEqualsMaps(sameMapping, indices.get(1).mapping());
        assertEquals(Set.of("b"), indices.get(1).concreteIndices());
    }

    public void testSeparateIncompatibleTypes() throws Exception {
        Map<String, EsField> basicMapping = loadMapping("mapping-basic.json", true);
        Map<String, EsField> incompatible = loadMapping("mapping-basic-incompatible.json");

        assertNotEquals(basicMapping, incompatible);

        List<EsIndex> indices = separate(new EsIndex("basic", basicMapping), new EsIndex("incompatible", incompatible));

        assertEquals(2, indices.size());
        assertEqualsMaps(basicMapping, indices.get(0).mapping());
        assertEquals(Set.of("basic"), indices.get(0).concreteIndices());
        assertEqualsMaps(incompatible, indices.get(1).mapping());
        assertEquals(Set.of("incompatible"), indices.get(1).concreteIndices());
    }

    // covers the scenario described in https://github.com/elastic/elasticsearch/issues/43876
    public void testMultipleCompatibleIndicesWithDifferentFields() {
        int indicesCount = randomIntBetween(2, 15);
        EsIndex[] expectedIndices = new EsIndex[indicesCount];

        // each index will have one field with different name than all others
        for (int i = 0; i < indicesCount; i++) {
            Map<String, EsField> mapping = Maps.newMapWithExpectedSize(1);
            String fieldName = "field" + (i + 1);
            mapping.put(fieldName, new KeywordEsField(fieldName));
            expectedIndices[i] = new EsIndex("org/elasticsearch/xpack/sql/index" + (i + 1), mapping);
        }
        Arrays.sort(expectedIndices, Comparator.comparing(EsIndex::name));

        List<EsIndex> actualIndices = separate(expectedIndices);
        actualIndices.sort(Comparator.comparing(EsIndex::name));
        assertEquals(indicesCount, actualIndices.size());
        for (int i = 0; i < indicesCount; i++) {
            assertEqualsMaps(expectedIndices[i].mapping(), actualIndices.get(i).mapping());
            assertEquals(Set.of(expectedIndices[i].name()), actualIndices.get(i).concreteIndices());
        }
    }

    public void testMergeConcreteIndices() {
        int indicesCount = randomIntBetween(2, 15);
        EsIndex[] expectedIndices = new EsIndex[indicesCount];
        Set<String> indexNames = new HashSet<>();

        for (int i = 0; i < indicesCount; i++) {
            Map<String, EsField> mapping = Maps.newMapWithExpectedSize(1);
            String fieldName = "field" + (i + 1);
            mapping.put(fieldName, new KeywordEsField(fieldName));
            String indexName = "org/elasticsearch/xpack/sql/index" + (i + 1);
            expectedIndices[i] = new EsIndex(indexName, mapping, Set.of(indexName));
            indexNames.add(indexName);
        }

        IndexResolution resolution = merge(expectedIndices);
        assertEquals(indicesCount, resolution.get().mapping().size());
        assertEquals(indexNames, resolution.get().concreteIndices());
    }

    public void testIndexWithNoMapping() {
        Map<String, Map<String, FieldCapabilities>> versionFC = singletonMap(
            "_version",
            singletonMap(
                "_index",
                new FieldCapabilities("_version", "_version", true, false, false, null, null, null, Collections.emptyMap())
            )
        );
        assertTrue(mergedMappings("*", new String[] { "empty" }, versionFC).isValid());
    }

    public void testMergeObjectIncompatibleTypes() throws Exception {
        var response = readFieldCapsResponse("fc-incompatible-object-compatible-subfields.json");

        IndexResolution resolution = IndexResolver.mergedMappings(
            SqlDataTypeRegistry.INSTANCE,
            "*",
            response,
            (fieldName, types) -> null,
            IndexResolver.PRESERVE_PROPERTIES,
            null
        );

        assertTrue(resolution.isValid());
        EsIndex esIndex = resolution.get();
        assertEquals(Set.of("index-1", "index-2"), esIndex.concreteIndices());
        EsField esField = esIndex.mapping().get("file");
        assertEquals(InvalidMappedField.class, esField.getClass());

        assertEquals(
            "mapped as [2] incompatible types: [keyword] in [index-2], [object] in [index-1]",
            ((InvalidMappedField) esField).errorMessage()
        );

        esField = esField.getProperties().get("name");
        assertNotNull(esField);
        assertEquals(esField.getDataType(), KEYWORD);
        assertEquals(KeywordEsField.class, esField.getClass());
    }

    public void testMergeObjectUnsupportedTypes() throws Exception {
        var response = readFieldCapsResponse("fc-unsupported-object-compatible-subfields.json");

        IndexResolution resolution = IndexResolver.mergedMappings(
            SqlDataTypeRegistry.INSTANCE,
            "*",
            response,
            (fieldName, types) -> null,
            IndexResolver.PRESERVE_PROPERTIES,
            null
        );

        assertTrue(resolution.isValid());
        EsIndex esIndex = resolution.get();
        assertEquals(Set.of("index-1", "index-2"), esIndex.concreteIndices());
        EsField esField = esIndex.mapping().get("file");
        assertEquals(InvalidMappedField.class, esField.getClass());

        assertEquals(
            "mapped as [2] incompatible types: [unknown] in [index-2], [object] in [index-1]",
            ((InvalidMappedField) esField).errorMessage()
        );

        esField = esField.getProperties().get("name");
        assertNotNull(esField);
        assertEquals(esField.getDataType(), UNSUPPORTED);
        assertEquals(UnsupportedEsField.class, esField.getClass());
    }

    private static FieldCapabilitiesResponse readFieldCapsResponse(String resourceName) throws IOException {
        InputStream stream = IndexResolverTests.class.getResourceAsStream("/" + resourceName);
        BytesReference ref = Streams.readFully(stream);
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, ref, XContentType.JSON)) {
            return FieldCapsUtils.parseFieldCapsResponse(parser);
        }
    }

    public static IndexResolution merge(EsIndex... indices) {
        return mergedMappings("*", Stream.of(indices).map(EsIndex::name).toArray(String[]::new), fromMappings(indices));
    }

    public static List<EsIndex> separate(EsIndex... indices) {
        return separateMappings(null, Stream.of(indices).map(EsIndex::name).toArray(String[]::new), fromMappings(indices));
    }

    public static Map<String, Map<String, FieldCapabilities>> fromMappings(EsIndex... indices) {
        Map<String, Map<String, FieldCapabilities>> merged = new HashMap<>();

        // first pass: create the field caps
        for (EsIndex index : indices) {
            for (EsField field : index.mapping().values()) {
                addFieldCaps(null, field, index.name(), merged);
            }
        }

        // second pass: update indices
        for (Entry<String, Map<String, FieldCapabilities>> entry : merged.entrySet()) {
            String fieldName = entry.getKey();
            Map<String, FieldCapabilities> caps = entry.getValue();
            if (entry.getValue().size() > 1) {
                for (EsIndex index : indices) {
                    EsField field = index.mapping().get(fieldName);
                    UpdateableFieldCapabilities fieldCaps = (UpdateableFieldCapabilities) caps.get(field.getDataType().esType());
                    fieldCaps.indices.add(index.name());
                }
                // TODO: what about nonAgg/SearchIndices?
            }
        }

        return merged;
    }

    private static void addFieldCaps(String parent, EsField field, String indexName, Map<String, Map<String, FieldCapabilities>> merged) {
        String fieldName = parent != null ? parent + "." + field.getName() : field.getName();
        Map<String, FieldCapabilities> map = merged.get(fieldName);
        if (map == null) {
            map = new HashMap<>();
            merged.put(fieldName, map);
        }
        FieldCapabilities caps = map.computeIfAbsent(
            field.getDataType().esType(),
            esType -> new UpdateableFieldCapabilities(
                fieldName,
                esType,
                isSearchable(field.getDataType()),
                isAggregatable(field.getDataType())
            )
        );

        if (field.isAggregatable() == false) {
            ((UpdateableFieldCapabilities) caps).nonAggregatableIndices.add(indexName);
        }

        for (EsField nested : field.getProperties().values()) {
            addFieldCaps(fieldName, nested, indexName, merged);
        }
    }

    private static boolean isSearchable(DataType type) {
        return isPrimitive(type);
    }

    private static boolean isAggregatable(DataType type) {
        return type.isNumeric() || type == KEYWORD || isDateTime(type);
    }

    private static class UpdateableFieldCapabilities extends FieldCapabilities {
        List<String> indices = new ArrayList<>();
        List<String> nonSearchableIndices = new ArrayList<>();
        List<String> nonAggregatableIndices = new ArrayList<>();

        UpdateableFieldCapabilities(String name, String type, boolean isSearchable, boolean isAggregatable) {
            super(name, type, false, isSearchable, isAggregatable, null, null, null, Collections.emptyMap());
        }

        @Override
        public String[] indices() {
            return indices.isEmpty() ? null : indices.toArray(new String[indices.size()]);
        }

        @Override
        public String[] nonSearchableIndices() {
            return nonSearchableIndices.isEmpty() ? null : nonSearchableIndices.toArray(new String[nonSearchableIndices.size()]);
        }

        @Override
        public String[] nonAggregatableIndices() {
            return nonAggregatableIndices.isEmpty() ? null : nonAggregatableIndices.toArray(new String[nonAggregatableIndices.size()]);
        }

        @Override
        public String toString() {
            return format("{},{}->{}", getName(), getType(), indices);
        }
    }

    private static <K, V> void assertEqualsMaps(Map<K, V> left, Map<K, V> right) {
        for (Entry<K, V> entry : left.entrySet()) {
            V rv = right.get(entry.getKey());
            assertEquals(format("Key [{}] has different values", entry.getKey()), entry.getValue(), rv);
        }
    }

    private void addFieldCaps(
        Map<String, Map<String, FieldCapabilities>> fieldCaps,
        String name,
        String type,
        boolean isSearchable,
        boolean isAggregatable
    ) {
        addFieldCaps(fieldCaps, name, type, false, isSearchable, isAggregatable);
    }

    private void addFieldCaps(
        Map<String, Map<String, FieldCapabilities>> fieldCaps,
        String name,
        String type,
        boolean isMetadataField,
        boolean isSearchable,
        boolean isAggregatable
    ) {
        Map<String, FieldCapabilities> cap = new HashMap<>();
        cap.put(
            type,
            new FieldCapabilities(name, type, isMetadataField, isSearchable, isAggregatable, null, null, null, Collections.emptyMap())
        );
        fieldCaps.put(name, cap);
    }

    private static IndexResolution mergedMappings(
        String indexPattern,
        String[] indexNames,
        Map<String, Map<String, FieldCapabilities>> fieldCaps
    ) {
        return IndexResolver.mergedMappings(
            SqlDataTypeRegistry.INSTANCE,
            indexPattern,
            new FieldCapabilitiesResponse(indexNames, fieldCaps)
        );
    }

    private static List<EsIndex> separateMappings(
        String javaRegex,
        String[] indexNames,
        Map<String, Map<String, FieldCapabilities>> fieldCaps
    ) {
        return IndexResolver.separateMappings(
            SqlDataTypeRegistry.INSTANCE,
            javaRegex,
            new FieldCapabilitiesResponse(indexNames, fieldCaps),
            null
        );
    }
}
