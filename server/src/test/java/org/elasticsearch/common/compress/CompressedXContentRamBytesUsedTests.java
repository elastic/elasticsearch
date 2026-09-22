/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.compress;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class CompressedXContentRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return CompressedXContent.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("bytes", "sha256");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        try {
            return CompressedXContent.fromJSON(randomMappingJson(randomIntBetween(1, 32)));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Non-tautology check: the estimate must cover at least the raw compressed payload plus the sha256 string content. These lower bounds
     * are derived independently of {@code ramBytesUsed()}'s own formula, so a systematically under-counting implementation would fail here.
     */
    public void testRamBytesUsedCoversRawPayload() {
        CompressedXContent content = (CompressedXContent) createRandomTestInstance();
        long rawLowerBound = (long) content.compressed().length + content.getSha256().length();
        assertThat(content.ramBytesUsed(), greaterThan(rawLowerBound));
    }

    public void testRamBytesUsedGrowsWithPayload() throws IOException {
        CompressedXContent small = CompressedXContent.fromJSON(randomMappingJson(1));
        CompressedXContent large = CompressedXContent.fromJSON(randomMappingJson(200));
        assertThat(large.ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }

    private static String randomMappingJson(int fieldCount) {
        StringBuilder json = new StringBuilder("{ \"_doc\": { \"properties\": {");
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append("\"field").append(i).append("\": { \"type\": \"keyword\" }");
        }
        json.append("} } }");
        return json.toString();
    }
}
