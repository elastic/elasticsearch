/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class IngestDocumentMatcher {

    private IngestDocumentMatcher() {
        // utility class
    }

    /**
     * Helper method to assert the equivalence between two IngestDocuments.
     *
     * @param expected first document to compare
     * @param actual second document to compare
     */
    public static void assertIngestDocument(IngestDocument expected, IngestDocument actual) {
        // trivially true: if they're both null, then all is well
        if (expected == null && actual == null) {
            return;
        }

        // if only one is null, however, then that's not okay
        if ((expected == null || actual == null)) {
            throw new AssertionError("Expected [" + expected + "] but received [" + actual + "].");
        }

        if ((deepEquals(expected.getIngestMetadata(), actual.getIngestMetadata(), true)
            && deepEquals(expected.getSourceAndMetadata(), actual.getSourceAndMetadata(), false)) == false) {
            throw new AssertionError("Expected [" + expected + "] but received [" + actual + "].");
        }
    }

    private static boolean deepEquals(Object a, Object b, boolean isIngestMeta) {
        if (a instanceof Map<?, ?> mapA) {
            if (b instanceof Map == false) {
                return false;
            }
            Map<?, ?> mapB = (Map<?, ?>) b;
            if (mapA.size() != mapB.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : mapA.entrySet()) {
                Object key = entry.getKey();
                // Don't compare the timestamp of ingest metadata since it will differ between executions
                if ((isIngestMeta && "timestamp".equals(key)) == false && deepEquals(entry.getValue(), mapB.get(key), false) == false) {
                    return false;
                }
            }
            return true;
        } else if (a instanceof List<?> listA) {
            if (b instanceof List == false) {
                return false;
            }
            List<?> listB = (List<?>) b;
            int countA = listA.size();
            if (countA != listB.size()) {
                return false;
            }
            for (int i = 0; i < countA; i++) {
                Object value = listA.get(i);
                if (deepEquals(value, listB.get(i), false) == false) {
                    return false;
                }
            }
            return true;
        } else {
            return Objects.deepEquals(a, b);
        }
    }
}
