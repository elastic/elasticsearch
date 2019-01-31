/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;

/**
 * Unit tests for the audit utils class
 */
public class AuditUtilTests extends ESTestCase {

    public void testIndicesRequest() {
        assertNull(AuditUtil.indices(new MockIndicesRequest(null, (String[])null)));
        final int numberOfIndices = randomIntBetween(1, 100);
        List<String> expectedIndices = new ArrayList<>();
        final boolean includeDuplicates = randomBoolean();
        for (int i = 0; i < numberOfIndices; i++) {
            String name = randomAlphaOfLengthBetween(1, 30);
            expectedIndices.add(name);
            if (includeDuplicates) {
                expectedIndices.add(name);
            }
        }
        final Set<String> uniqueExpectedIndices = new HashSet<>(expectedIndices);
        final Set<String> result = AuditUtil.indices(new MockIndicesRequest(null,
                expectedIndices.toArray(new String[expectedIndices.size()])));
        assertNotNull(result);
        assertEquals(uniqueExpectedIndices.size(), result.size());
        assertThat(result, hasItems(uniqueExpectedIndices.toArray(Strings.EMPTY_ARRAY)));
    }
}
