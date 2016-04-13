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
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ScoreModeTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(FiltersFunctionScoreQuery.ScoreMode.FIRST.ordinal(), equalTo(0));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.AVG.ordinal(), equalTo(1));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.MAX.ordinal(), equalTo(2));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.SUM.ordinal(), equalTo(3));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.MIN.ordinal(), equalTo(4));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.MULTIPLY.ordinal(), equalTo(5));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            FiltersFunctionScoreQuery.ScoreMode.FIRST.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            FiltersFunctionScoreQuery.ScoreMode.AVG.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            FiltersFunctionScoreQuery.ScoreMode.MAX.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            FiltersFunctionScoreQuery.ScoreMode.SUM.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            FiltersFunctionScoreQuery.ScoreMode.MIN.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(4));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            FiltersFunctionScoreQuery.ScoreMode.MULTIPLY.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(in.readVInt(), equalTo(5));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(FiltersFunctionScoreQuery.ScoreMode.readFromStream(in), equalTo(FiltersFunctionScoreQuery.ScoreMode.FIRST));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(FiltersFunctionScoreQuery.ScoreMode.readFromStream(in), equalTo(FiltersFunctionScoreQuery.ScoreMode.AVG));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(FiltersFunctionScoreQuery.ScoreMode.readFromStream(in), equalTo(FiltersFunctionScoreQuery.ScoreMode.MAX));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(FiltersFunctionScoreQuery.ScoreMode.readFromStream(in), equalTo(FiltersFunctionScoreQuery.ScoreMode.SUM));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(4);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(FiltersFunctionScoreQuery.ScoreMode.readFromStream(in), equalTo(FiltersFunctionScoreQuery.ScoreMode.MIN));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(5);
            try (StreamInput in = StreamInput.wrap(out.bytes())) {
                assertThat(FiltersFunctionScoreQuery.ScoreMode.readFromStream(in), equalTo(FiltersFunctionScoreQuery.ScoreMode.MULTIPLY));
            }
        }
    }

    public void testFromString() {
        assertThat(FiltersFunctionScoreQuery.ScoreMode.fromString("first"), equalTo(FiltersFunctionScoreQuery.ScoreMode.FIRST));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.fromString("avg"), equalTo(FiltersFunctionScoreQuery.ScoreMode.AVG));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.fromString("max"), equalTo(FiltersFunctionScoreQuery.ScoreMode.MAX));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.fromString("sum"), equalTo(FiltersFunctionScoreQuery.ScoreMode.SUM));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.fromString("min"), equalTo(FiltersFunctionScoreQuery.ScoreMode.MIN));
        assertThat(FiltersFunctionScoreQuery.ScoreMode.fromString("multiply"), equalTo(FiltersFunctionScoreQuery.ScoreMode.MULTIPLY));
    }
}
