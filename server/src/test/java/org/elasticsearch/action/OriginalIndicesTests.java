/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class OriginalIndicesTests extends ESTestCase {

    private static final IndicesOptions[] indicesOptionsValues = new IndicesOptions[] {
        IndicesOptions.lenientExpandOpen(),
        IndicesOptions.strictExpand(),
        IndicesOptions.strictExpandOpen(),
        IndicesOptions.strictExpandOpenAndForbidClosed(),
        IndicesOptions.strictSingleIndexNoExpandForbidClosed() };

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
            if (out.getVersion().onOrAfter(Version.V_7_7_0) || originalIndices.indicesOptions().expandWildcardsHidden()) {
                assertThat(originalIndices2.indicesOptions(), equalTo(originalIndices.indicesOptions()));
            } else if (originalIndices.indicesOptions().expandWildcardsHidden()) {
                assertThat(originalIndices2.indicesOptions(), equalTo(originalIndices.indicesOptions()));
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
