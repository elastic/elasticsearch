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

package org.elasticsearch.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class OriginalIndicesTests extends ElasticsearchTestCase {

    private static final IndicesOptions[] indicesOptionsValues = new IndicesOptions[]{
            IndicesOptions.lenientExpandOpen() , IndicesOptions.strictExpand(), IndicesOptions.strictExpandOpen(),
            IndicesOptions.strictExpandOpenAndForbidClosed(), IndicesOptions.strictSingleIndexNoExpandForbidClosed()};

    @Test
    public void testOriginalIndicesSerialization() throws IOException {
        int iterations = iterations(10, 30);
        for (int i = 0; i < iterations; i++) {
            OriginalIndices originalIndices = randomOriginalIndices();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomVersion());
            OriginalIndices.writeOriginalIndices(originalIndices, out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(out.getVersion());
            OriginalIndices originalIndices2 = OriginalIndices.readOriginalIndices(in);

            if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                assertThat(originalIndices2.indices(), equalTo(originalIndices.indices()));
                assertThat(originalIndices2.indicesOptions(), equalTo(originalIndices.indicesOptions()));
            } else {
                assertThat(originalIndices2.indices(), nullValue());
                assertThat(originalIndices2.indicesOptions(), nullValue());
            }
        }
    }

    @Test
    public void testOptionalOriginalIndicesSerialization() throws IOException {
        int iterations = iterations(10, 30);
        for (int i = 0; i < iterations; i++) {
            OriginalIndices originalIndices;
            boolean missing = randomBoolean();
            if (missing) {
                originalIndices = randomOriginalIndices();
            } else {
                originalIndices = OriginalIndices.EMPTY;
            }

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomVersion());
            OriginalIndices.writeOptionalOriginalIndices(originalIndices, out);

            BytesStreamInput in = new BytesStreamInput(out.bytes());
            in.setVersion(out.getVersion());
            OriginalIndices originalIndices2 = OriginalIndices.readOptionalOriginalIndices(in);

            if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                assertThat(originalIndices2.indices(), equalTo(originalIndices.indices()));
                assertThat(originalIndices2.indicesOptions(), equalTo(originalIndices.indicesOptions()));
            } else {
                assertThat(originalIndices2.indices(), nullValue());
                assertThat(originalIndices2.indicesOptions(), nullValue());
            }
        }
    }

    private static OriginalIndices randomOriginalIndices() {
        int numIndices = randomInt(10);
        String[] indices = new String[numIndices];
        for (int j = 0; j < indices.length; j++) {
            indices[j] = randomAsciiOfLength(randomIntBetween(1, 10));
        }
        IndicesOptions indicesOptions = randomFrom(indicesOptionsValues);
        return new OriginalIndices(indices, indicesOptions);
    }
}
