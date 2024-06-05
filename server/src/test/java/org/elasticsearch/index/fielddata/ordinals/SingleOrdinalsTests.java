/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
