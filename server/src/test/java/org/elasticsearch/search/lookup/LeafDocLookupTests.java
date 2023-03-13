/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SourceValueFetcherIndexFieldData;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.index.mapper.MappedFieldType.FielddataOperation.SCRIPT;
import static org.elasticsearch.index.mapper.MappedFieldType.FielddataOperation.SEARCH;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafDocLookupTests extends ESTestCase {
    private ScriptDocValues<?> docValues;
    private LeafDocLookup docLookup;
    private Consumer<Integer> nextDocCallback;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        docValues = mock(ScriptDocValues.class);
        nextDocCallback = i -> {}; // do nothing by default

        MappedFieldType fieldType1 = mock(MappedFieldType.class);
        when(fieldType1.name()).thenReturn("field");
        when(fieldType1.valueForDisplay(any())).then(returnsFirstArg());
        IndexFieldData<?> fieldData1 = createFieldData(docValues, "field");

        MappedFieldType fieldType2 = mock(MappedFieldType.class);
        when(fieldType1.name()).thenReturn("alias");
        when(fieldType1.valueForDisplay(any())).then(returnsFirstArg());
        IndexFieldData<?> fieldData2 = createFieldData(docValues, "alias");

        docLookup = new LeafDocLookup(
            field -> field.equals("field") ? fieldType1 : field.equals("alias") ? fieldType2 : null,
            (fieldType, fielddataType) -> fieldType == fieldType1 ? fieldData1 : fieldType == fieldType2 ? fieldData2 : null,
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

        FlattenedFieldMapper fieldMapper = new FlattenedFieldMapper.Builder("field").build(MapperBuilderContext.root(false));
        DynamicFieldType fieldType = fieldMapper.fieldType();
        MappedFieldType fieldType1 = fieldType.getChildFieldType("key1");
        MappedFieldType fieldType2 = fieldType.getChildFieldType("key2");

        BiFunction<MappedFieldType, MappedFieldType.FielddataOperation, IndexFieldData<?>> fieldDataSupplier = (ft, fdt) -> {
            FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) ft;
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
                nextDocCallback.accept(id);
            }
        };
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        doReturn(delegateDocValuesField).when(leafFieldData).getScriptFieldFactory(name);

        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn(name);
        doReturn(leafFieldData).when(fieldData).load(any());

        return fieldData;
    }

    public void testParallelCache() {
        String nameDoc = "doc"; // field where search and script return doc values
        String nameSource = "source"; // field where search returns no data and script returns source values
        String nameDocAndSource = "docAndSource"; // field where search returns doc values and script returns source values

        MappedFieldType docMappedFieldType = mock(MappedFieldType.class);
        MappedFieldType sourceMappedFieldType = mock(MappedFieldType.class);
        MappedFieldType docAndSourceMappedFieldType = mock(MappedFieldType.class);

        Map<String, MappedFieldType> namesToMappedFieldTypes = Map.of(
            nameDoc,
            docMappedFieldType,
            nameSource,
            sourceMappedFieldType,
            nameDocAndSource,
            docAndSourceMappedFieldType
        );

        IndexFieldData<?> docIndexFieldData = mock(IndexFieldData.class);
        SourceValueFetcherIndexFieldData<?> sourceIndexFieldData = mock(SourceValueFetcherIndexFieldData.class);
        IndexFieldData<?> docAndSourceDocIndexFieldData = mock(IndexFieldData.class);
        SourceValueFetcherIndexFieldData<?> docAndSourceSourceIndexFieldData = mock(SourceValueFetcherIndexFieldData.class);

        LeafFieldData docLeafFieldData = mock(LeafFieldData.class);
        LeafFieldData sourceLeafFieldData = mock(SourceValueFetcherIndexFieldData.SourceValueFetcherLeafFieldData.class);
        LeafFieldData docAndSourceDocLeafFieldData = mock(LeafFieldData.class);
        LeafFieldData docAndSourceSourceLeafFieldData = mock(SourceValueFetcherIndexFieldData.SourceValueFetcherLeafFieldData.class);

        DocValuesScriptFieldFactory docFactory = mock(DocValuesScriptFieldFactory.class);
        DocValuesScriptFieldFactory sourceFactory = mock(DocValuesScriptFieldFactory.class);
        DocValuesScriptFieldFactory docAndSourceDocFactory = mock(DocValuesScriptFieldFactory.class);
        DocValuesScriptFieldFactory docAndSourceSourceFactory = mock(DocValuesScriptFieldFactory.class);

        ScriptDocValues<?> docDocValues = mock(ScriptDocValues.class);
        Field<?> fieldDocValues = mock(Field.class);
        Field<?> fieldSourceValues = mock(Field.class);
        ScriptDocValues<?> docSourceAndDocValues = mock(ScriptDocValues.class);
        Field<?> fieldSourceAndDocValues = mock(Field.class);

        doReturn(docLeafFieldData).when(docIndexFieldData).load(any());
        doReturn(docFactory).when(docLeafFieldData).getScriptFieldFactory(nameDoc);
        doReturn(docDocValues).when(docFactory).toScriptDocValues();
        doReturn(fieldDocValues).when(docFactory).toScriptField();

        doReturn(sourceLeafFieldData).when(sourceIndexFieldData).load(any());
        doReturn(sourceFactory).when(sourceLeafFieldData).getScriptFieldFactory(nameSource);
        doReturn(fieldSourceValues).when(sourceFactory).toScriptField();

        doReturn(docAndSourceDocLeafFieldData).when(docAndSourceDocIndexFieldData).load(any());
        doReturn(docAndSourceDocFactory).when(docAndSourceDocLeafFieldData).getScriptFieldFactory(nameDocAndSource);
        doReturn(docSourceAndDocValues).when(docAndSourceDocFactory).toScriptDocValues();

        doReturn(docAndSourceSourceLeafFieldData).when(docAndSourceSourceIndexFieldData).load(any());
        doReturn(docAndSourceSourceFactory).when(docAndSourceSourceLeafFieldData).getScriptFieldFactory(nameDocAndSource);
        doReturn(fieldSourceAndDocValues).when(docAndSourceSourceFactory).toScriptField();

        LeafDocLookup leafDocLookup = new LeafDocLookup(namesToMappedFieldTypes::get, (mappedFieldType, operation) -> {
            if (mappedFieldType.equals(docMappedFieldType)) {
                if (operation == SEARCH || operation == SCRIPT) {
                    return docIndexFieldData;
                } else {
                    throw new IllegalArgumentException("unknown operation [" + operation + "]");
                }
            } else if (mappedFieldType.equals(sourceMappedFieldType)) {
                if (operation == SEARCH) {
                    throw new IllegalArgumentException("search cannot access source");
                } else if (operation == SCRIPT) {
                    return sourceIndexFieldData;
                } else {
                    throw new IllegalArgumentException("unknown operation [" + operation + "]");
                }
            } else if (mappedFieldType.equals(docAndSourceMappedFieldType)) {
                if (operation == SEARCH) {
                    return docAndSourceDocIndexFieldData;
                } else if (operation == SCRIPT) {
                    return docAndSourceSourceIndexFieldData;
                } else {
                    throw new IllegalArgumentException("unknown operation [" + operation + "]");
                }
            } else {
                throw new IllegalArgumentException("unknown mapped field type [" + mappedFieldType + "]");
            }
        }, null);

        // load shared doc values field into cache w/ doc-access first
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertTrue(leafDocLookup.fieldFactoryCache.isEmpty());
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);

        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);

        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);

        // clear the cache
        leafDocLookup.docFactoryCache.clear();
        leafDocLookup.fieldFactoryCache.clear();

        // load shared doc values field into cache w/ field-access first
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertTrue(leafDocLookup.docFactoryCache.isEmpty());
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);

        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);

        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);

        // clear the cache
        leafDocLookup.docFactoryCache.clear();
        leafDocLookup.fieldFactoryCache.clear();

        // load source values field into cache
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        expectThrows(IllegalArgumentException.class, () -> leafDocLookup.get(nameSource));
        assertTrue(leafDocLookup.docFactoryCache.isEmpty());
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);

        // clear the cache
        leafDocLookup.docFactoryCache.clear();
        leafDocLookup.fieldFactoryCache.clear();

        // load doc values for doc-access and script values for script-access from the same index field data w/ doc-access first
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertTrue(leafDocLookup.fieldFactoryCache.isEmpty());
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);

        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);

        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);

        // clear the cache
        leafDocLookup.docFactoryCache.clear();
        leafDocLookup.fieldFactoryCache.clear();

        // load doc values for doc-access and script values for script-access from the same index field data w/ field-access first
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertTrue(leafDocLookup.docFactoryCache.isEmpty());
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);

        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);

        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(1, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertEquals(1, leafDocLookup.docFactoryCache.size());
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);

        // clear the cache
        leafDocLookup.docFactoryCache.clear();
        leafDocLookup.fieldFactoryCache.clear();

        // add all 3 fields to the cache w/ doc-access first
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(2, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertEquals(3, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);

        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(3, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertEquals(2, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);

        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(2, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertEquals(3, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);

        // clear the cache
        leafDocLookup.docFactoryCache.clear();
        leafDocLookup.fieldFactoryCache.clear();

        // add all 3 fields to the cache w/ field-access first
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(3, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertEquals(2, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);

        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(2, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
        assertEquals(3, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);

        assertEquals(fieldDocValues, leafDocLookup.getScriptField(nameDoc));
        assertEquals(fieldSourceValues, leafDocLookup.getScriptField(nameSource));
        assertEquals(fieldSourceAndDocValues, leafDocLookup.getScriptField(nameDocAndSource));
        assertEquals(docDocValues, leafDocLookup.get(nameDoc));
        assertEquals(docSourceAndDocValues, leafDocLookup.get(nameDocAndSource));
        assertEquals(3, leafDocLookup.fieldFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.fieldFactoryCache.get(nameDoc).factory);
        assertEquals(sourceFactory, leafDocLookup.fieldFactoryCache.get(nameSource).factory);
        assertEquals(docAndSourceSourceFactory, leafDocLookup.fieldFactoryCache.get(nameDocAndSource).factory);
        assertEquals(2, leafDocLookup.docFactoryCache.size());
        assertEquals(docFactory, leafDocLookup.docFactoryCache.get(nameDoc).factory);
        assertEquals(docAndSourceDocFactory, leafDocLookup.docFactoryCache.get(nameDocAndSource).factory);
    }

    public void testLookupPrivilegesAdvanceDoc() {
        nextDocCallback = i -> SpecialPermission.check();

        // mimic the untrusted codebase, which gets no permissions
        var restrictedContext = new AccessControlContext(new ProtectionDomain[] { new ProtectionDomain(null, null) });
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            ScriptDocValues<?> fetchedDocValues = docLookup.get("field");
            assertEquals(docValues, fetchedDocValues);
            return null;
        }, restrictedContext);
    }
}
