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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.core.KeywordFieldMapper.KeywordFieldType;

import java.io.IOException;

public class KeywordFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new KeywordFieldMapper.KeywordFieldType();
    }

    public void testIsFieldWithinQuery() throws IOException {
        KeywordFieldType ft = new KeywordFieldType();
        // current impl ignores args and shourd always return INTERSECTS
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(null,
                RandomStrings.randomAsciiOfLengthBetween(random(), 0, 5),
                RandomStrings.randomAsciiOfLengthBetween(random(), 0, 5),
                randomBoolean(), randomBoolean(), null, null));
    }
}
