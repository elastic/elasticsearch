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

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class IndexTemplateMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexTemplateMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("name", "version", "patterns", "settings", "mappings", "aliases");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // Settings.estimatedRamBytesUsed() deliberately omits interned keys/values; a full-graph RamUsageTester walk would count them
        // and fail estimate >= actual by design.
        return false;
    }

    /**
     * Non-tautology check: a template with more index patterns and an alias must report a larger size than a minimal one.
     */
    public void testRamBytesUsedGrowsWithContent() {
        IndexTemplateMetadata small = IndexTemplateMetadata.builder("t").patterns(List.of("a-*")).build();
        IndexTemplateMetadata large = IndexTemplateMetadata.builder("t")
            .patterns(List.of("a-*", "b-*", "c-*"))
            .putAlias(AliasMetadata.builder("alias").build())
            .build();
        assertThat(large.ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
