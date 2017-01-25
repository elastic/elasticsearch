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
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class IngestDocumentMatcher {
    /**
     * Helper method to assert the equivalence between two IngestDocuments.
     *
     * @param a first object to compare
     * @param b second object to compare
     */
    public static void assertIngestDocument(Object a, Object b) {
        if (a instanceof Map) {
            Map<?, ?> mapA = (Map<?, ?>) a;
            Map<?, ?> mapB = (Map<?, ?>) b;
            for (Map.Entry<?, ?> entry : mapA.entrySet()) {
                if (entry.getValue() instanceof List || entry.getValue() instanceof Map) {
                    assertIngestDocument(entry.getValue(), mapB.get(entry.getKey()));
                }
            }
        } else if (a instanceof List) {
            List<?> listA = (List<?>) a;
            List<?> listB = (List<?>) b;
            for (int i = 0; i < listA.size(); i++) {
                Object value = listA.get(i);
                if (value instanceof List || value instanceof Map) {
                    assertIngestDocument(value, listB.get(i));
                }
            }
        } else if (a instanceof byte[]) {
            assertArrayEquals((byte[]) a, (byte[])b);
        } else if (a instanceof IngestDocument) {
            IngestDocument docA = (IngestDocument) a;
            IngestDocument docB = (IngestDocument) b;
            assertIngestDocument(docA.getSourceAndMetadata(), docB.getSourceAndMetadata());
            assertIngestDocument(docA.getIngestMetadata(), docB.getIngestMetadata());
        } else {
            String msg = String.format(Locale.ROOT, "Expected %s class to be equal to %s", a.getClass().getName(), b.getClass().getName());
            assertThat(msg, a, equalTo(b));
        }
    }
}
