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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.EnumSet;

import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class OriginalIndicesTests extends ESTestCase {

    private static final IndicesOptions[] indicesOptionsValues = new IndicesOptions[]{
            IndicesOptions.lenientExpandOpen() , IndicesOptions.strictExpand(), IndicesOptions.strictExpandOpen(),
            IndicesOptions.strictExpandOpenAndForbidClosed(), IndicesOptions.strictSingleIndexNoExpandForbidClosed()};

    public void testOriginalIndicesSerialization() throws IOException {
        int iterations = iterations(10, 30);
        for (int i = 0; i < iterations; i++) {
            OriginalIndices originalIndices = randomOriginalIndices();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomCompatibleVersion(random(), Version.CURRENT));
            OriginalIndices.writeOriginalIndices(originalIndices, out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(out.getVersion());
            OriginalIndices originalIndices2 = OriginalIndices.readOriginalIndices(in);

            assertThat(originalIndices2.indices(), equalTo(originalIndices.indices()));
            // indices options are not equivalent when sent to an older version and re-read due
            // to the addition of hidden indices as expand to hidden indices is always true when
            // read from a prior version
            // Options changes in 8.0.0 (for now, fix on backport!)
            if (out.getVersion().before(Version.V_8_0_0)) {
                EnumSet<IndicesOptions.Option> expectedOptions = originalIndices.indicesOptions().getOptions();
                expectedOptions.add(IndicesOptions.Option.ALLOW_SYSTEM_INDEX_ACCESS);
                assertEquals(expectedOptions, originalIndices2.indicesOptions().getOptions());
            } else {
                assertEquals(originalIndices.indicesOptions().getOptions(), originalIndices2.indicesOptions().getOptions());
            }

            // Wildcard states changed in 7.7.0
            if (out.getVersion().before(Version.V_7_7_0)) {
                EnumSet<IndicesOptions.WildcardStates> expectedWildcardOptions = originalIndices.indicesOptions().getExpandWildcards();
                expectedWildcardOptions.add(IndicesOptions.WildcardStates.HIDDEN);
                assertEquals(expectedWildcardOptions, originalIndices2.indicesOptions().getExpandWildcards());
            } else {
                assertEquals(originalIndices.indicesOptions().getExpandWildcards(), originalIndices2.indicesOptions().getExpandWildcards());
            }
        }
    }

    public static OriginalIndices randomOriginalIndices() {
        int numIndices = randomInt(10);
        String[] indices = new String[numIndices];
        for (int j = 0; j < indices.length; j++) {
            indices[j] = randomAlphaOfLength(randomIntBetween(1, 10));
        }
        IndicesOptions indicesOptions = randomFrom(indicesOptionsValues);
        return new OriginalIndices(indices, indicesOptions);
    }
}
