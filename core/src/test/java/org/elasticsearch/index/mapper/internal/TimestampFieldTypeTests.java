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
package org.elasticsearch.index.mapper.internal;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyDateFieldTypeTests;

public class TimestampFieldTypeTests extends LegacyDateFieldTypeTests {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new TimestampFieldMapper.TimestampFieldType();
    }

    @Override
    public void testValueForSearch() {
        MappedFieldType ft = createDefaultFieldType();
        String date = "2015-10-12T12:09:55.000Z";
        long instant = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(date).getMillis();
        assertEquals(instant, ft.valueForSearch(instant));
    }
}
