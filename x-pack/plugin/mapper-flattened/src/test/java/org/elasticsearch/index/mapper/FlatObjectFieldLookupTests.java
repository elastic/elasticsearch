/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlatObjectFieldLookupTests extends ESTestCase {

    public void testFieldTypeLookup() {
        String fieldName = "object1.object2.field";
        FlatObjectFieldMapper mapper = createFlatObjectMapper(fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll(singletonList(mapper), emptyList());
        assertEquals(mapper.fieldType(), lookup.get(fieldName));

        String objectKey = "key1.key2";
        String searchFieldName = fieldName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertEquals(mapper.keyedFieldName(), searchFieldType.name());
        assertThat(searchFieldType, instanceOf(KeyedFlatObjectFieldType.class));

        FlatObjectFieldMapper.KeyedFlatObjectFieldType keyedFieldType = (KeyedFlatObjectFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());
    }

    public void testFieldTypeLookupWithAlias() {
        String fieldName = "object1.object2.field";
        FlatObjectFieldMapper mapper = createFlatObjectMapper(fieldName);

        String aliasName = "alias";
        FieldAliasMapper alias = new FieldAliasMapper(aliasName, aliasName, fieldName);

        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll(singletonList(mapper), singletonList(alias));
        assertEquals(mapper.fieldType(), lookup.get(aliasName));

        String objectKey = "key1.key2";
        String searchFieldName = aliasName + "." + objectKey;

        MappedFieldType searchFieldType = lookup.get(searchFieldName);
        assertEquals(mapper.keyedFieldName(), searchFieldType.name());
        assertThat(searchFieldType, instanceOf(KeyedFlatObjectFieldType.class));

        KeyedFlatObjectFieldType keyedFieldType = (KeyedFlatObjectFieldType) searchFieldType;
        assertEquals(objectKey, keyedFieldType.key());
    }

    public void testFieldTypeLookupWithMultipleFields() {
        String field1 = "object1.object2.field";
        String field2 = "object1.field";
        String field3 = "object2.field";

        FlatObjectFieldMapper mapper1 = createFlatObjectMapper(field1);
        FlatObjectFieldMapper mapper2 = createFlatObjectMapper(field2);
        FlatObjectFieldMapper mapper3 = createFlatObjectMapper(field3);

        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll(Arrays.asList(mapper1, mapper2), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));

        lookup = lookup.copyAndAddAll(singletonList(mapper3), emptyList());
        assertNotNull(lookup.get(field1 + ".some.key"));
        assertNotNull(lookup.get(field2 + ".some.key"));
        assertNotNull(lookup.get(field3 + ".some.key"));
    }

    public void testMaxDynamicKeyDepth() {
        Map<String, DynamicKeyFieldMapper> mappers = new HashMap<>();
        Map<String, String> aliases = new HashMap<>();
        assertEquals(0, DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Add a flattened object field.
        String flatObjectName = "object1.object2.field";
        FlatObjectFieldMapper flatObjectField = createFlatObjectMapper(flatObjectName);
        mappers.put(flatObjectName, flatObjectField);
        assertEquals(3, DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Add a short alias to that field.
        String aliasName = "alias";
        aliases.put(aliasName, flatObjectName);
        assertEquals(3,  DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Add a longer alias to that field.
        String longAliasName = "object1.object2.object3.alias";
        aliases.put(longAliasName, flatObjectName);
        assertEquals(4,  DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));

        // Update the long alias to refer to a non-flattened object field.
        String fieldName = "field";
        aliases.put(longAliasName, fieldName);
        assertEquals(3,  DynamicKeyFieldTypeLookup.getMaxKeyDepth(mappers, aliases));
    }

    public void testFieldLookupIterator() {
        MockFieldMapper mapper = new MockFieldMapper("foo");
        FlatObjectFieldMapper flatObjectMapper = createFlatObjectMapper("object1.object2.field");

        FieldTypeLookup lookup = new FieldTypeLookup()
            .copyAndAddAll(Arrays.asList(mapper, flatObjectMapper), emptyList());

        Set<String> fieldNames = new HashSet<>();
        for (MappedFieldType fieldType : lookup) {
            fieldNames.add(fieldType.name());
        }

        assertThat(fieldNames, containsInAnyOrder(
            mapper.name(), flatObjectMapper.name(), flatObjectMapper.keyedFieldName()));
    }

    private FlatObjectFieldMapper createFlatObjectMapper(String fieldName) {
        Settings settings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());
        return new FlatObjectFieldMapper.Builder(fieldName).build(context);
    }

    public void testScriptDocValuesLookup() {
        MapperService mapperService = mock(MapperService.class);

        ScriptDocValues<?> docValues1 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData1 = createFieldData(docValues1);

        ScriptDocValues<?> docValues2 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData2 = createFieldData(docValues2);

        KeyedFlatObjectFieldType fieldType1 = new KeyedFlatObjectFieldType("key1");
        when(mapperService.fieldType("json.key1")).thenReturn(fieldType1);

        KeyedFlatObjectFieldType fieldType2 = new KeyedFlatObjectFieldType( "key2");
        when(mapperService.fieldType("json.key2")).thenReturn(fieldType2);

        Function<MappedFieldType, IndexFieldData<?>> fieldDataSupplier = fieldType -> {
            KeyedFlatObjectFieldType keyedFieldType = (KeyedFlatObjectFieldType) fieldType;
            return keyedFieldType.key().equals("key1") ? fieldData1 : fieldData2;
        };

        SearchLookup searchLookup = new SearchLookup(mapperService, fieldDataSupplier);
        LeafDocLookup docLookup = searchLookup.doc().getLeafDocLookup(null);

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
