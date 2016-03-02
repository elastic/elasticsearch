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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ValuesSourceTypeTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(ValuesSourceType.ANY.ordinal(), equalTo(0));
        assertThat(ValuesSourceType.NUMERIC.ordinal(), equalTo(1));
        assertThat(ValuesSourceType.BYTES.ordinal(), equalTo(2));
        assertThat(ValuesSourceType.GEOPOINT.ordinal(), equalTo(3));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ValuesSourceType.ANY.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ValuesSourceType.NUMERIC.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ValuesSourceType.BYTES.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ValuesSourceType.GEOPOINT.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(ValuesSourceType.ANY.readFrom(in), equalTo(ValuesSourceType.ANY));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(ValuesSourceType.ANY.readFrom(in), equalTo(ValuesSourceType.NUMERIC));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(ValuesSourceType.ANY.readFrom(in), equalTo(ValuesSourceType.BYTES));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(ValuesSourceType.ANY.readFrom(in), equalTo(ValuesSourceType.GEOPOINT));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(4, Integer.MAX_VALUE));
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                ValuesSourceType.ANY.readFrom(in);
                fail("Expected IOException");
            } catch(IOException e) {
                assertThat(e.getMessage(), containsString("Unknown ValuesSourceType ordinal ["));
            }

        }
    }
}
