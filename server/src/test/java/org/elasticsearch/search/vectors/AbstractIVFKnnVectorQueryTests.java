/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class AbstractIVFKnnVectorQueryTests extends ESTestCase {

    /** Every doc appears once with its best score, shifted by the doc base, whatever the input order of its copies. */
    public void testKeepBestCopiesIsOrderIndependent() {
        List<ScoreDoc> hits = Arrays.asList(
            new ScoreDoc(3, 0.5f),
            new ScoreDoc(7, 0.9f),
            new ScoreDoc(3, 0.8f),
            new ScoreDoc(1, 0.2f),
            new ScoreDoc(7, 0.4f)
        );
        Collections.shuffle(hits, random());
        TopDocs in = new TopDocs(new TotalHits(5, TotalHits.Relation.EQUAL_TO), hits.toArray(ScoreDoc[]::new));

        TopDocs out = AbstractIVFKnnVectorQuery.keepBestCopies(in, 100);

        ScoreDoc[] sorted = out.scoreDocs.clone();
        Arrays.sort(sorted, Comparator.comparingInt(sd -> sd.doc));
        assertEquals(3, sorted.length);
        assertEquals(101, sorted[0].doc);
        assertEquals(0.2f, sorted[0].score, 0f);
        assertEquals(103, sorted[1].doc);
        assertEquals(0.8f, sorted[1].score, 0f);
        assertEquals(107, sorted[2].doc);
        assertEquals(0.9f, sorted[2].score, 0f);
        assertEquals(in.totalHits, out.totalHits);
    }
}
