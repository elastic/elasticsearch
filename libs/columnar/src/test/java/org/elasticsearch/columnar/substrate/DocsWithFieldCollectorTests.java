/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DocsWithFieldCollectorTests extends ESTestCase {

    public void testNothingCollected() throws IOException {
        final DocsWithFieldCollector collector = new DocsWithFieldCollector(between(1, 100));
        assertEquals(0, collector.count());
        assertEquals(List.of(), docs(collector));
    }

    /** Documents from 0 without a gap are remembered by their count alone. */
    public void testPrefixOfDocuments() throws IOException {
        final int count = between(1, 100);
        final DocsWithFieldCollector collector = new DocsWithFieldCollector(count + between(0, 100));
        final List<Integer> expected = new ArrayList<>();
        for (int doc = 0; doc < count; doc++) {
            collector.add(doc);
            expected.add(doc);
        }
        assertEquals(count, collector.count());
        assertEquals(expected, docs(collector));
    }

    /** A gap, whether it comes first, in the middle or last, is what makes the collector keep a bit set. */
    public void testDocumentsWithGaps() throws IOException {
        final int maxDoc = between(2, 5_000);
        final List<Integer> expected = new ArrayList<>();
        final DocsWithFieldCollector collector = new DocsWithFieldCollector(maxDoc);
        final int density = between(1, 10);
        final int start = randomBoolean() ? 0 : between(0, maxDoc - 1);
        for (int doc = start; doc < maxDoc; doc++) {
            if (doc == start || between(0, density) == 0) {
                collector.add(doc);
                expected.add(doc);
            }
        }
        assertEquals(expected.size(), collector.count());
        assertEquals(expected, docs(collector));
    }

    /** A run from 0 that then breaks keeps what came before the break. */
    public void testPrefixThenGap() throws IOException {
        final DocsWithFieldCollector collector = new DocsWithFieldCollector(100);
        collector.add(0);
        collector.add(1);
        collector.add(2);
        collector.add(50);
        collector.add(99);
        assertEquals(5, collector.count());
        assertEquals(List.of(0, 1, 2, 50, 99), docs(collector));
    }

    private static List<Integer> docs(DocsWithFieldCollector collector) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        final DocIdSetIterator iterator = collector.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }
}
