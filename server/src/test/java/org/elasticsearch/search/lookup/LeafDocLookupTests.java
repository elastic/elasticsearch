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
import org.elasticsearch.index.mapper.DynamicMappedField;
import org.elasticsearch.index.mapper.MappedField;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Function;

import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
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

        docValues = mock(ScriptDocValues.class);

        MappedFieldType fieldType1 = mock(MappedFieldType.class);
        when(fieldType1.valueForDisplay(any())).then(returnsFirstArg());
        MappedField field1 = new MappedField("field", fieldType1);
        IndexFieldData<?> fieldData1 = createFieldData(docValues, "field");

        MappedFieldType fieldType2 = mock(MappedFieldType.class);
        when(fieldType1.valueForDisplay(any())).then(returnsFirstArg());
        MappedField field2 = new MappedField("alias", fieldType2);
        IndexFieldData<?> fieldData2 = createFieldData(docValues, "alias");

        docLookup = new LeafDocLookup(
            field -> field.equals("field") ? field1 : field.equals("alias") ? field2 : null,
            field -> field == field1 ? fieldData1 : field == field2 ? fieldData2 : null,
            null
        );
    }

    public void testBasicLookup() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("field");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testFieldAliases() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("alias");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testFlattenedField() throws IOException {
        ScriptDocValues<?> docValues1 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData1 = createFieldData(docValues1, "flattened.key1");

        ScriptDocValues<?> docValues2 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData2 = createFieldData(docValues2, "flattened.key2");

        FlattenedFieldMapper fieldMapper = new FlattenedFieldMapper.Builder("field").build(MapperBuilderContext.ROOT);
        DynamicMappedField dynamicMappedField = (DynamicMappedField) fieldMapper.field();
        MappedField fieldType1 = dynamicMappedField.getChildField("key1");
        MappedField fieldType2 = dynamicMappedField.getChildField("key2");

        Function<MappedField, IndexFieldData<?>> fieldDataSupplier = ft -> {
            FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) ft.type();
            return keyedFieldType.key().equals("key1") ? fieldData1 : fieldData2;
        };

        LeafDocLookup docLookup = new LeafDocLookup(field -> {
            if (field.equals("flattened.key1")) {
                return fieldType1;
            }
            if (field.equals("flattened.key2")) {
                return fieldType2;
            }
            return null;
        }, fieldDataSupplier, null);

        assertEquals(docValues1, docLookup.get("flattened.key1"));
        assertEquals(docValues2, docLookup.get("flattened.key2"));
    }

    private IndexFieldData<?> createFieldData(ScriptDocValues<?> scriptDocValues, String name) throws IOException {
        DelegateDocValuesField delegateDocValuesField = new DelegateDocValuesField(scriptDocValues, name) {
            @Override
            public void setNextDocId(int id) {
                // do nothing
            }
        };
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        doReturn(delegateDocValuesField).when(leafFieldData).getScriptFieldFactory(name);

        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(name);
        doReturn(leafFieldData).when(fieldData).load(any());

        return fieldData;
    }
}
