/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;

/**
 * Unit tests for the audit utils class
 */
public class AuditUtilTests extends ESTestCase {

    public void testIndicesRequest() {
        assertNull(AuditUtil.indices(new MockIndicesRequest(null)));
        final int numberOfIndices = randomIntBetween(1, 100);
        List<String> expectedIndices = new ArrayList<>();
        final boolean includeDuplicates = randomBoolean();
        for (int i = 0; i < numberOfIndices; i++) {
            String name = randomAsciiOfLengthBetween(1, 30);
            expectedIndices.add(name);
            if (includeDuplicates) {
                expectedIndices.add(name);
            }
        }
        final Set<String> uniqueExpectedIndices = new HashSet<>(expectedIndices);
        final Set<String> result = AuditUtil.indices(new MockIndicesRequest(expectedIndices.toArray(Strings.EMPTY_ARRAY)));
        assertNotNull(result);
        assertEquals(uniqueExpectedIndices.size(), result.size());
        assertThat(result, hasItems(uniqueExpectedIndices.toArray(Strings.EMPTY_ARRAY)));
    }

    public void testCompositeIndicesRequest() {
        assertNull(AuditUtil.indices(new MockCompositeIndicesRequest(Collections.emptyList())));
        assertNull(AuditUtil.indices(new MockCompositeIndicesRequest(Collections.singletonList(new MockIndicesRequest(null)))));
        final int numberOfIndicesRequests = randomIntBetween(1, 10);
        final boolean includeDuplicates = randomBoolean();
        List<String> expectedIndices = new ArrayList<>();
        List<IndicesRequest> indicesRequests = new ArrayList<>(numberOfIndicesRequests);
        for (int i = 0; i < numberOfIndicesRequests; i++) {
            final int numberOfIndices = randomIntBetween(1, 12);
            List<String> indices = new ArrayList<>(numberOfIndices);
            for (int j = 0; j < numberOfIndices; j++) {
                String name = randomAsciiOfLengthBetween(1, 30);
                indices.add(name);
                if (includeDuplicates) {
                    indices.add(name);
                }
            }
            expectedIndices.addAll(indices);
            indicesRequests.add(new MockIndicesRequest(indices.toArray(Strings.EMPTY_ARRAY)));
        }

        final Set<String> uniqueExpectedIndices = new HashSet<>(expectedIndices);
        final Set<String> result = AuditUtil.indices(new MockCompositeIndicesRequest(indicesRequests));
        assertNotNull(result);
        assertEquals(uniqueExpectedIndices.size(), result.size());
        assertThat(result, hasItems(uniqueExpectedIndices.toArray(Strings.EMPTY_ARRAY)));
    }

    private static class MockIndicesRequest extends TransportMessage implements IndicesRequest {

        private final String[] indices;

        private MockIndicesRequest(String[] indices) {
            this.indices = indices;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return null;
        }
    }

    private static class MockCompositeIndicesRequest extends TransportMessage implements CompositeIndicesRequest {

        private final List<? extends IndicesRequest> requests;

        private MockCompositeIndicesRequest(List<? extends IndicesRequest> requests) {
            this.requests = requests;
        }

        @Override
        public List<? extends IndicesRequest> subRequests() {
            return requests;
        }
    }
}
