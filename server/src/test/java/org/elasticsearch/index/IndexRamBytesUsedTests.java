/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;

public class IndexRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return Index.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("name", "uuid");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of();
    }

    @Override
    protected Accountable createRandomTestInstance() {
        return new Index("index-name", UUIDs.randomBase64UUID());
    }

    /**
     * Non-tautology check: longer index name and uuid must increase the reported size.
     */
    public void testRamBytesUsedGrowsWithStringLengths() {
        Index small = new Index("a", "b");
        assertThat(createRandomTestInstance().ramBytesUsed(), greaterThan(small.ramBytesUsed()));
    }
}
