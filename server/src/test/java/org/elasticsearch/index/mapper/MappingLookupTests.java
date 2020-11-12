/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;

public class MappingLookupTests extends ESTestCase {

    public void testOnlyRuntimeField() {
        MappingLookup mappingLookup = new MappingLookup(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new TestRuntimeField("test")), 0);
        assertEquals(0, size(mappingLookup.fieldMappers()));
        assertEquals(0, mappingLookup.objectMappers().size());
        assertNull(mappingLookup.getMapper("test"));
        assertThat(mappingLookup.fieldTypes().get("test"), instanceOf(TestRuntimeField.class));
    }

    public void testRuntimeFieldLeafOverride() {
        MockFieldMapper fieldMapper = new MockFieldMapper("test");
        MappingLookup mappingLookup = new MappingLookup(Collections.singletonList(fieldMapper), Collections.emptyList(),
            Collections.emptyList(), Collections.singletonList(new TestRuntimeField("test")), 0);
        assertThat(mappingLookup.getMapper("test"), instanceOf(MockFieldMapper.class));
        assertEquals(1, size(mappingLookup.fieldMappers()));
        assertEquals(0, mappingLookup.objectMappers().size());
        assertThat(mappingLookup.fieldTypes().get("test"), instanceOf(TestRuntimeField.class));
        assertEquals(1, size(mappingLookup.fieldTypes().filter(ft -> true)));
    }

    public void testSubfieldOverride() {
        MockFieldMapper fieldMapper = new MockFieldMapper("object.subfield");
        ObjectMapper objectMapper = new ObjectMapper("object", "object", new Explicit<>(true, true), ObjectMapper.Nested.NO,
            ObjectMapper.Dynamic.TRUE, Collections.singletonMap("object.subfield", fieldMapper), Version.CURRENT);
        MappingLookup mappingLookup = new MappingLookup(Collections.singletonList(fieldMapper), Collections.singletonList(objectMapper),
            Collections.emptyList(), Collections.singletonList(new TestRuntimeField("object.subfield")), 0);
        assertThat(mappingLookup.getMapper("object.subfield"), instanceOf(MockFieldMapper.class));
        assertEquals(1, size(mappingLookup.fieldMappers()));
        assertEquals(1, mappingLookup.objectMappers().size());
        assertThat(mappingLookup.fieldTypes().get("object.subfield"), instanceOf(TestRuntimeField.class));
        assertEquals(1, size(mappingLookup.fieldTypes().filter(ft -> true)));
    }

    private static int size(Iterable<?> iterable) {
        int count = 0;
        for (Object obj : iterable) {
            count++;
        }
        return count;
    }
}
