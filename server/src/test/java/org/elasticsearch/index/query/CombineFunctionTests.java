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

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CombineFunctionTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(CombineFunction.MULTIPLY.ordinal(), equalTo(0));
        assertThat(CombineFunction.REPLACE.ordinal(), equalTo(1));
        assertThat(CombineFunction.SUM.ordinal(), equalTo(2));
        assertThat(CombineFunction.AVG.ordinal(), equalTo(3));
        assertThat(CombineFunction.MIN.ordinal(), equalTo(4));
        assertThat(CombineFunction.MAX.ordinal(), equalTo(5));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            CombineFunction.MULTIPLY.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            CombineFunction.REPLACE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            CombineFunction.SUM.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            CombineFunction.AVG.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            CombineFunction.MIN.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(4));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            CombineFunction.MAX.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(5));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(CombineFunction.readFromStream(in), equalTo(CombineFunction.MULTIPLY));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(CombineFunction.readFromStream(in), equalTo(CombineFunction.REPLACE));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(CombineFunction.readFromStream(in), equalTo(CombineFunction.SUM));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(CombineFunction.readFromStream(in), equalTo(CombineFunction.AVG));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(4);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(CombineFunction.readFromStream(in), equalTo(CombineFunction.MIN));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(5);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(CombineFunction.readFromStream(in), equalTo(CombineFunction.MAX));
            }
        }
    }

    public void testFromString() {
        assertThat(CombineFunction.fromString("multiply"), equalTo(CombineFunction.MULTIPLY));
        assertThat(CombineFunction.fromString("replace"), equalTo(CombineFunction.REPLACE));
        assertThat(CombineFunction.fromString("sum"), equalTo(CombineFunction.SUM));
        assertThat(CombineFunction.fromString("avg"), equalTo(CombineFunction.AVG));
        assertThat(CombineFunction.fromString("min"), equalTo(CombineFunction.MIN));
        assertThat(CombineFunction.fromString("max"), equalTo(CombineFunction.MAX));
    }
}
