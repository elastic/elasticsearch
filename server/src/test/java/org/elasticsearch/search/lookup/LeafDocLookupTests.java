/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.function.Function;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafDocLookupTests extends ESTestCase {
    private ScriptDocValues<?> docValues;
    private LeafDocLookup docLookup;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        when(fieldType.valueForDisplay(anyObject())).then(returnsFirstArg());

        docValues = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData = createFieldData(docValues);

        docLookup = new LeafDocLookup(field -> field.equals("field") || field.equals("alias") ? fieldType : null,
            ignored -> fieldData, null);
    }

    public void testBasicLookup() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("field");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testFieldAliases() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("alias");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testFlattenedField() {
        ScriptDocValues<?> docValues1 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData1 = createFieldData(docValues1);

        ScriptDocValues<?> docValues2 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData2 = createFieldData(docValues2);

        FlattenedFieldMapper fieldMapper = new FlattenedFieldMapper.Builder("field").build(new ContentPath(1));
        FlattenedFieldMapper.KeyedFlattenedFieldType fieldType1 = fieldMapper.keyedFieldType("key1");
        FlattenedFieldMapper.KeyedFlattenedFieldType fieldType2 = fieldMapper.keyedFieldType("key2");

        Function<MappedFieldType, IndexFieldData<?>> fieldDataSupplier = fieldType -> {
            FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) fieldType;
            return keyedFieldType.key().equals("key1") ? fieldData1 : fieldData2;
        };

        LeafDocLookup docLookup = new LeafDocLookup(field -> {
            if (field.equals("json.key1")) {
                return fieldType1;
            }
            if (field.equals("json.key2")) {
                return fieldType2;
            }
            return null;
        }, fieldDataSupplier, null);

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
