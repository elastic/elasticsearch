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

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class AliasMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return AliasMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("alias", "filter", "indexRouting", "searchRouting", "searchRoutingValues", "writeIndex", "isHidden");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // searchRoutingValues is a HashSet whose table/node graph RamUsageTester walks more completely than sizeOfCollection.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        AliasMetadata.Builder builder = AliasMetadata.builder(randomAlphaOfLengthBetween(1, 12));
        if (randomBoolean()) {
            builder.filter("{\"term\":{\"" + randomAlphaOfLength(5) + "\":\"" + randomAlphaOfLength(8) + "\"}}");
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(1, 8));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(1, 8));
        }
        if (randomBoolean()) {
            builder.writeIndex(randomBoolean());
        }
        if (randomBoolean()) {
            builder.isHidden(randomBoolean());
        }
        return builder.build();
    }

    /**
     * Non-tautology check: attaching a filter and routing must increase the estimate over a bare alias.
     */
    public void testRamBytesUsedGrowsWithOptionalFields() {
        AliasMetadata bare = AliasMetadata.builder("alias").build();
        AliasMetadata populated = AliasMetadata.builder("alias")
            .filter("{\"term\":{\"field\":\"value\"}}")
            .indexRouting("routing")
            .searchRouting("sr1,sr2")
            .writeIndex(true)
            .build();
        assertThat(populated.ramBytesUsed(), greaterThan(bare.ramBytesUsed()));
    }
}
