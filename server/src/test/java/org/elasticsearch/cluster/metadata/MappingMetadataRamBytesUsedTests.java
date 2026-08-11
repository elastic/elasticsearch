/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class MappingMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return MappingMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("type", "source");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createTestInstance() {
        try {
            return new MappingMetadata(CompressedXContent.fromJSON("""
                { "_doc": { "properties": {
                  "a": { "type": "keyword" }, "b": { "type": "keyword" }, "c": { "type": "text" }, "d": { "type": "long" }
                } } }
                """));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Non-tautology check: the estimate must include the compressed mapping source, so a larger mapping is larger on heap.
     */
    public void testRamBytesUsedIncludesSource() throws IOException {
        MappingMetadata small = new MappingMetadata(CompressedXContent.fromJSON("""
            { "_doc": { "properties": { "a": { "type": "keyword" } } } }
            """));
        assertThat(createTestInstance().ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
