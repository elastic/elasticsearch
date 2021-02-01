/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.flattened.mapper.FlattenedFieldMapper;
import org.elasticsearch.xpack.flattened.mapper.FlattenedFieldMapper.KeyedFlattenedFieldType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlattenedFieldLookupTests extends ESTestCase {

    public void testFieldTypeLookup() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup("_doc", singletonList(mapper), emptyList(), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(fieldName));

        String objectKey = "key1.key2";
        String searchFieldName = fieldName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertEquals(mapper.keyedFieldName(), searchFieldType.name());
        assertThat(searchFieldType, instanceOf(KeyedFlattenedFieldType.class));

        KeyedFlattenedFieldType keyedFieldType = (KeyedFlattenedFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());
    }

    public void testFieldTypeLookupWithAlias() {
        String fieldName = "object1.object2.field";
        FlattenedFieldMapper mapper = createFlattenedMapper(fieldName);

        String aliasName = "alias";
        FieldAliasMapper alias = new FieldAliasMapper(aliasName, aliasName, fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup("_doc", singletonList(mapper), singletonList(alias), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(aliasName));

        String objectKey = "key1.key2";
        String searchFieldName = aliasName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertEquals(mapper.keyedFieldName(), searchFieldType.name());
        assertThat(searchFieldType, instanceOf(KeyedFlattenedFieldType.class));

        KeyedFlattenedFieldType keyedFieldType = (KeyedFlattenedFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());
    }

    public void testFieldTypeLookupWithMultipleFields() {
        String field1 = "object1.object2.field";
        String field2 = "object1.field";
        String field3 = "object2.field";

        FlattenedFieldMapper mapper1 = createFlattenedMapper(field1);
        FlattenedFieldMapper mapper2 = createFlattenedMapper(field2);
        FlattenedFieldMapper mapper3 = createFlattenedMapper(field3);

        FieldTypeLookup lookup = new FieldTypeLookup("_doc", Arrays.asList(mapper1, mapper2), emptyList(), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));

        lookup = new FieldTypeLookup("_doc", Arrays.asList(mapper1, mapper2, mapper3), emptyList(), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));
        assertNotNull(lookup.get(field3 + ".some.key"));
    }

    public void testMaxDynamicKeyDepth() {
        Map<String, DynamicKeyFieldMapper> mappers = new HashMap<>();
        Map<String, String> aliases = new HashMap<>();
        assertEquals(0, DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Add a flattened object field.
        String name = "object1.object2.field";
        FlattenedFieldMapper flattenedMapper = createFlattenedMapper(name);
        mappers.put(name, flattenedMapper);
        assertEquals(3, DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Add a short alias to that field.
        String aliasName = "alias";
        aliases.put(aliasName, name);
        assertEquals(3,  DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Add a longer alias to that field.
        String longAliasName = "object1.object2.object3.alias";
        aliases.put(longAliasName, name);
        assertEquals(4,  DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Update the long alias to refer to a non-flattened object field.
        String fieldName = "field";
        aliases.put(longAliasName, fieldName);
        assertEquals(3,  DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));
    }

    public void testFieldLookupIterator() {
        MockFieldMapper mapper = new MockFieldMapper("foo");
        FlattenedFieldMapper flattenedMapper = createFlattenedMapper("object1.object2.field");

        FieldTypeLookup lookup = new FieldTypeLookup("_doc", Arrays.asList(mapper, flattenedMapper), emptyList(), emptyList());

        Set<String> fieldNames = new HashSet<>();
        lookup.filter(ft -> true).forEach(ft -> fieldNames.add(ft.name()));

        assertThat(fieldNames, containsInAnyOrder(
            mapper.name(), flattenedMapper.name(), flattenedMapper.keyedFieldName()));
    }

    private FlattenedFieldMapper createFlattenedMapper(String fieldName) {
        return new FlattenedFieldMapper.Builder(fieldName).build(new ContentPath());
    }

    public void testScriptDocValuesLookup() {
        ScriptDocValues<?> docValues1 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData1 = createFieldData(docValues1);

        ScriptDocValues<?> docValues2 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData2 = createFieldData(docValues2);

        KeyedFlattenedFieldType fieldType1
            = new KeyedFlattenedFieldType("field", true, true, "key1", false, Collections.emptyMap());
        KeyedFlattenedFieldType fieldType2
            = new KeyedFlattenedFieldType( "field", true, true, "key2", false, Collections.emptyMap());

        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataSupplier = (fieldType, searchLookup) -> {
            KeyedFlattenedFieldType keyedFieldType = (KeyedFlattenedFieldType) fieldType;
            return keyedFieldType.key().equals("key1") ? fieldData1 : fieldData2;
        };

        SearchLookup searchLookup = new SearchLookup(field -> {
            if (field.equals("json.key1")) {
                return fieldType1;
            }
            if (field.equals("json.key2")) {
                return fieldType2;
            }
            return null;
        }, fieldDataSupplier);
        LeafDocLookup docLookup = searchLookup.getLeafSearchLookup(null).doc();

        assertEquals(docValues1, docLookup.get("json.key1"));
        assertEquals(docValues2, docLookup.get("json.key2"));
    }

    private IndexFieldData<?> createFieldData(ScriptDocValues<?> scriptDocValues) {
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        doReturn(scriptDocValues).when(leafFieldData).getScriptValues();

        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn("field");
        doReturn(leafFieldData).when(fieldData).load(anyObject());

        return fieldData;
    }
}
