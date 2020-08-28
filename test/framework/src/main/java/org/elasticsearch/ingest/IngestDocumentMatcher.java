/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IngestDocumentMatcher {
    /**
     * Helper method to assert the equivalence between two IngestDocuments.
     *
     * @param docA first document to compare
     * @param docB second document to compare
     */
    public static void assertIngestDocument(IngestDocument docA, IngestDocument docB) {
        if ((deepEquals(docA.getIngestMetadata(), docB.getIngestMetadata(), true) &&
            deepEquals(docA.getSourceAndMetadata(), docB.getSourceAndMetadata(), false)) == false) {
            throw new AssertionError("Expected [" + docA + "] but received [" + docB + "].");
        }
    }

    private static boolean deepEquals(Object a, Object b, boolean isIngestMeta) {
        if (a instanceof Map) {
            Map<?, ?> mapA = (Map<?, ?>) a;
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
                if ((isIngestMeta && "timestamp".equals(key)) == false
                    && deepEquals(entry.getValue(), mapB.get(key), false) == false) {
                    return false;
                }
            }
            return true;
        } else if (a instanceof List) {
            List<?> listA = (List<?>) a;
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
