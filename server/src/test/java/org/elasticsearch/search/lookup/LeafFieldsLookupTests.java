/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafFieldsLookupTests extends ESTestCase {
    private LeafFieldsLookup fieldsLookup;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        // Add 10 when valueForDisplay is called so it is easy to be sure it *was* called
        when(fieldType.valueForDisplay(anyObject())).then(invocation ->
                (Double) invocation.getArguments()[0] + 10);

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("field")).thenReturn(fieldType);
        when(mapperService.fieldType("alias")).thenReturn(fieldType);

        FieldInfo mockFieldInfo = new FieldInfo("field", 1, false, false, true,
            IndexOptions.NONE, DocValuesType.NONE, -1, Collections.emptyMap(), 0, 0, 0, false);

        LeafReader leafReader = mock(LeafReader.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            StoredFieldVisitor visitor = (StoredFieldVisitor) args[1];
            visitor.doubleField(mockFieldInfo, 2.718);
            return null;
        }).when(leafReader).document(anyInt(), any(StoredFieldVisitor.class));

        fieldsLookup = new LeafFieldsLookup(mapperService, leafReader);
    }

    public void testBasicLookup() {
        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("field");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }

    public void testLookupWithFieldAlias() {
        FieldLookup fieldLookup = (FieldLookup) fieldsLookup.get("alias");
        assertEquals("field", fieldLookup.fieldType().name());

        List<Object> values = fieldLookup.getValues();
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals(12.718, values.get(0));
    }
}
