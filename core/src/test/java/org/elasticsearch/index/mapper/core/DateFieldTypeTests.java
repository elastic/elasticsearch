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

import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Before;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class DateFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new DateFieldMapper.DateFieldType();
    }

    @Before
    public void setupProperties() {
        setDummyNullValue(10);
        addModifier(new Modifier("format", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((DateFieldMapper.DateFieldType) ft).setDateTimeFormatter(Joda.forPattern("basic_week_date", Locale.ROOT));
            }
        });
        addModifier(new Modifier("locale", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((DateFieldMapper.DateFieldType) ft).setDateTimeFormatter(Joda.forPattern("date_optional_time", Locale.CANADA));
            }
        });
        addModifier(new Modifier("numeric_resolution", true, true) {
            @Override
            public void modify(MappedFieldType ft) {
                ((DateFieldMapper.DateFieldType)ft).setTimeUnit(TimeUnit.HOURS);
            }
        });
    }
}
