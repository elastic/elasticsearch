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
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class InferenceFieldMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return InferenceFieldMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("name", "inferenceId", "searchInferenceId", "sourceFields", "chunkingSettings");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        String name = randomAlphaOfLengthBetween(3, 10);
        String inferenceId = randomIdentifier();
        String searchInferenceId = randomIdentifier();
        String[] sourceFields = generateRandomStringArray(randomIntBetween(1, 5), 10, false, false);
        Map<String, Object> chunkingSettings = InferenceFieldMetadataTests.generateRandomChunkingSettings();
        return new InferenceFieldMetadata(name, inferenceId, searchInferenceId, sourceFields, chunkingSettings);
    }

    /**
     * Non-tautology check: more source fields and chunking settings must increase the estimate.
     */
    public void testRamBytesUsedGrowsWithSourceFieldsAndChunkingSettings() {
        InferenceFieldMetadata minimal = new InferenceFieldMetadata("field", "inference", "inference", new String[0], null);
        InferenceFieldMetadata populated = new InferenceFieldMetadata(
            "field",
            "inference-id-with-more-chars",
            "search-inference-id-with-more-chars",
            new String[] { "source1", "source2", "source3" },
            Map.of("strategy", "word_boundary", "max_chunk_size", 100, "overlap", 10)
        );
        assertThat(populated.ramBytesUsed(), greaterThan(minimal.ramBytesUsed()));
    }
}
