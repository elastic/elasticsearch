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

public class ValuesSourceTypeTests extends AbstractWriteableEnumTestCase {

    public ValuesSourceTypeTests() {
        super(ValuesSourceType::fromStream);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(ValuesSourceType.ANY.ordinal(), equalTo(0));
        assertThat(ValuesSourceType.NUMERIC.ordinal(), equalTo(1));
        assertThat(ValuesSourceType.BYTES.ordinal(), equalTo(2));
        assertThat(ValuesSourceType.GEOPOINT.ordinal(), equalTo(3));
        assertThat(ValuesSourceType.RANGE.ordinal(), equalTo(4));
    }

    @Override
    public void testFromString() {
        assertThat(ValuesSourceType.fromString("any"), equalTo(ValuesSourceType.ANY));
        assertThat(ValuesSourceType.fromString("numeric"), equalTo(ValuesSourceType.NUMERIC));
        assertThat(ValuesSourceType.fromString("bytes"), equalTo(ValuesSourceType.BYTES));
        assertThat(ValuesSourceType.fromString("geopoint"), equalTo(ValuesSourceType.GEOPOINT));
        assertThat(ValuesSourceType.fromString("range"), equalTo(ValuesSourceType.RANGE));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ValuesSourceType.fromString("does_not_exist"));
        assertThat(e.getMessage(),
            equalTo("No enum constant org.elasticsearch.search.aggregations.support.ValuesSourceType.DOES_NOT_EXIST"));
        expectThrows(NullPointerException.class, () -> ValuesSourceType.fromString(null));
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, ValuesSourceType.ANY);
        assertReadFromStream(1, ValuesSourceType.NUMERIC);
        assertReadFromStream(2, ValuesSourceType.BYTES);
        assertReadFromStream(3, ValuesSourceType.GEOPOINT);
        assertReadFromStream(4, ValuesSourceType.RANGE);
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(ValuesSourceType.ANY, 0);
        assertWriteToStream(ValuesSourceType.NUMERIC, 1);
        assertWriteToStream(ValuesSourceType.BYTES, 2);
        assertWriteToStream(ValuesSourceType.GEOPOINT, 3);
        assertWriteToStream(ValuesSourceType.RANGE, 4);
    }
}
