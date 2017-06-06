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

package org.elasticsearch.action.support;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndicesOptionsTests extends ESTestCase {
    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

            BytesStreamOutput output = new BytesStreamOutput();
            Version outputVersion = randomVersion(random());
            output.setVersion(outputVersion);
            indicesOptions.writeIndicesOptions(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(randomVersion(random()));
            IndicesOptions indicesOptions2 = IndicesOptions.readIndicesOptions(streamInput);

            assertThat(indicesOptions2.ignoreUnavailable(), equalTo(indicesOptions.ignoreUnavailable()));
            assertThat(indicesOptions2.allowNoIndices(), equalTo(indicesOptions.allowNoIndices()));
            assertThat(indicesOptions2.expandWildcardsOpen(), equalTo(indicesOptions.expandWildcardsOpen()));
            assertThat(indicesOptions2.expandWildcardsClosed(), equalTo(indicesOptions.expandWildcardsClosed()));

            assertThat(indicesOptions2.forbidClosedIndices(), equalTo(indicesOptions.forbidClosedIndices()));
            assertThat(indicesOptions2.allowAliasesToMultipleIndices(), equalTo(indicesOptions.allowAliasesToMultipleIndices()));

            if (output.getVersion().onOrAfter(Version.V_6_0_0_alpha2)) {
                assertEquals(indicesOptions2.ignoreAliases(), indicesOptions.ignoreAliases());
            } else {
                assertFalse(indicesOptions2.ignoreAliases());
            }
        }
    }

    public void testFromOptions() {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            boolean ignoreUnavailable = randomBoolean();
            boolean allowNoIndices = randomBoolean();
            boolean expandToOpenIndices = randomBoolean();
            boolean expandToClosedIndices = randomBoolean();
            boolean allowAliasesToMultipleIndices = randomBoolean();
            boolean forbidClosedIndices = randomBoolean();
            boolean ignoreAliases = randomBoolean();

            IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                    ignoreUnavailable, allowNoIndices,expandToOpenIndices, expandToClosedIndices,
                    allowAliasesToMultipleIndices, forbidClosedIndices, ignoreAliases
            );

            assertThat(indicesOptions.ignoreUnavailable(), equalTo(ignoreUnavailable));
            assertThat(indicesOptions.allowNoIndices(), equalTo(allowNoIndices));
            assertThat(indicesOptions.expandWildcardsOpen(), equalTo(expandToOpenIndices));
            assertThat(indicesOptions.expandWildcardsClosed(), equalTo(expandToClosedIndices));
            assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
            assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
            assertThat(indicesOptions.forbidClosedIndices(), equalTo(forbidClosedIndices));
            assertEquals(ignoreAliases, indicesOptions.ignoreAliases());
        }
    }
}
