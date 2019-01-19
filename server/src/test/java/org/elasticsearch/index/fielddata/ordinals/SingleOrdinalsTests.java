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
package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SingleOrdinalsTests extends ESTestCase {
    public void testSvValues() throws IOException {
        int numDocs = 1000000;
        int numOrdinals = numDocs / 4;
        Map<Integer, Long> controlDocToOrdinal = new HashMap<>();
        OrdinalsBuilder builder = new OrdinalsBuilder(numDocs);
        long ordinal = builder.currentOrdinal();
        for (int doc = 0; doc < numDocs; doc++) {
            if (doc % numOrdinals == 0) {
                ordinal = builder.nextOrdinal();
            }
            controlDocToOrdinal.put(doc, ordinal);
            builder.addDoc(doc);
        }

        Ordinals ords = builder.build();
        assertThat(ords, instanceOf(SinglePackedOrdinals.class));
        SortedSetDocValues docs = ords.ordinals();
        final SortedDocValues singleOrds = DocValues.unwrapSingleton(docs);
        assertNotNull(singleOrds);

        for (Map.Entry<Integer, Long> entry : controlDocToOrdinal.entrySet()) {
            assertTrue(singleOrds.advanceExact(entry.getKey()));
            assertEquals(singleOrds.ordValue(), (long) entry.getValue());
        }
    }

    public void testMvOrdinalsTrigger() throws IOException {
        int numDocs = 1000000;
        OrdinalsBuilder builder = new OrdinalsBuilder(numDocs);
        builder.nextOrdinal();
        for (int doc = 0; doc < numDocs; doc++) {
            builder.addDoc(doc);
        }

        Ordinals ords = builder.build();
        assertThat(ords, instanceOf(SinglePackedOrdinals.class));

        builder.nextOrdinal();
        builder.addDoc(0);
        ords = builder.build();
        assertThat(ords, not(instanceOf(SinglePackedOrdinals.class)));
    }

}
