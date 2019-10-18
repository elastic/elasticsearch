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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.io.stream.AbstractWriteableEnumTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BuiltinValuesSourceTypeTests extends AbstractWriteableEnumTestCase {

    public BuiltinValuesSourceTypeTests() {
        super(BuiltinValuesSourceType::fromStream);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(BuiltinValuesSourceType.ANY.ordinal(), equalTo(0));
        assertThat(BuiltinValuesSourceType.NUMERIC.ordinal(), equalTo(1));
        assertThat(BuiltinValuesSourceType.BYTES.ordinal(), equalTo(2));
        assertThat(BuiltinValuesSourceType.GEOPOINT.ordinal(), equalTo(3));
        assertThat(BuiltinValuesSourceType.RANGE.ordinal(), equalTo(4));
    }

    @Override
    public void testFromString() {
        assertThat(BuiltinValuesSourceType.fromString("any"), equalTo(BuiltinValuesSourceType.ANY));
        assertThat(BuiltinValuesSourceType.fromString("numeric"), equalTo(BuiltinValuesSourceType.NUMERIC));
        assertThat(BuiltinValuesSourceType.fromString("bytes"), equalTo(BuiltinValuesSourceType.BYTES));
        assertThat(BuiltinValuesSourceType.fromString("geopoint"), equalTo(BuiltinValuesSourceType.GEOPOINT));
        assertThat(BuiltinValuesSourceType.fromString("range"), equalTo(BuiltinValuesSourceType.RANGE));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> BuiltinValuesSourceType.fromString("does_not_exist"));
        assertThat(e.getMessage(),
            equalTo("No enum constant org.elasticsearch.search.aggregations.support.BuiltinValuesSourceType.DOES_NOT_EXIST"));
        expectThrows(NullPointerException.class, () -> BuiltinValuesSourceType.fromString(null));
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, BuiltinValuesSourceType.ANY);
        assertReadFromStream(1, BuiltinValuesSourceType.NUMERIC);
        assertReadFromStream(2, BuiltinValuesSourceType.BYTES);
        assertReadFromStream(3, BuiltinValuesSourceType.GEOPOINT);
        assertReadFromStream(4, BuiltinValuesSourceType.RANGE);
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(BuiltinValuesSourceType.ANY, 0);
        assertWriteToStream(BuiltinValuesSourceType.NUMERIC, 1);
        assertWriteToStream(BuiltinValuesSourceType.BYTES, 2);
        assertWriteToStream(BuiltinValuesSourceType.GEOPOINT, 3);
        assertWriteToStream(BuiltinValuesSourceType.RANGE, 4);
    }
}
