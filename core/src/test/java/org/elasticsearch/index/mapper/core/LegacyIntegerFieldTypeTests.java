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
package org.elasticsearch.index.mapper.core;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.core.LegacyIntegerFieldMapper.IntegerFieldType;
import org.junit.Before;

import java.io.IOException;

public class LegacyIntegerFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new LegacyIntegerFieldMapper.IntegerFieldType();
    }

    @Before
    public void setupProperties() {
        setDummyNullValue(10);
    }

    public void testIsFieldWithinQuery() throws IOException {
        IntegerFieldType ft = new IntegerFieldType();
        // current impl ignores args and shourd always return INTERSECTS
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(null, randomInt(), randomInt(),
                randomBoolean(), randomBoolean(), null, null));
    }

    public void testValueForSearch() {
        MappedFieldType ft = createDefaultFieldType();
        assertEquals(Integer.valueOf(3), ft.valueForSearch(Integer.valueOf(3)));
    }
}
