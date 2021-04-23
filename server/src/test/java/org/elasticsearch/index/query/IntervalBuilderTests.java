/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class IntervalBuilderTests extends ESTestCase {

    private static final IntervalBuilder BUILDER = new IntervalBuilder("field1", new StandardAnalyzer()) {

        @Override
        protected IntervalsSource termIntervals(BytesRef term) {
            return Intervals.term(term);
        }

    };

    public void testSimpleTerm() throws IOException {

        CannedTokenStream ts = new CannedTokenStream(new Token("term1", 1, 2));

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.term("term1");

        assertEquals(expected, source);
    }

    public void testOrdered() throws IOException {

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 3, 4),
            new Token("term3", 5, 6)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.ordered(
            Intervals.term("term1"), Intervals.term("term2"), Intervals.term("term3")
        );

        assertEquals(expected, source);

    }

    public void testUnordered() throws IOException {

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 3, 4),
            new Token("term3", 5, 6)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, false);
        IntervalsSource expected = Intervals.unordered(
            Intervals.term("term1"), Intervals.term("term2"), Intervals.term("term3")
        );

        assertEquals(expected, source);

    }

    public void testPhrase() throws IOException {

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 3, 4),
            new Token("term3", 5, 6)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), 0, true);
        IntervalsSource expected = Intervals.phrase(
            Intervals.term("term1"), Intervals.term("term2"), Intervals.term("term3")
        );

        assertEquals(expected, source);

    }

    public void testPhraseWithStopword() throws IOException {

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 1, 2),
            new Token("term3", 2, 5, 6)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), 0, true);
        IntervalsSource expected = Intervals.phrase(
            Intervals.term("term1"), Intervals.extend(Intervals.term("term3"), 1, 0)
        );

        assertEquals(expected, source);

    }

    public void testEmptyTokenStream() throws IOException {
        CannedTokenStream ts = new CannedTokenStream();
        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), 0, true);
        assertSame(IntervalBuilder.NO_INTERVALS, source);
    }

    public void testSimpleSynonyms() throws IOException {

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 3, 4),
            new Token("term4", 0, 3, 4),
            new Token("term3", 5, 6)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.ordered(
            Intervals.term("term1"), Intervals.or(Intervals.term("term2"), Intervals.term("term4")), Intervals.term("term3")
        );

        assertEquals(expected, source);

    }

    public void testSimpleSynonymsWithGap() throws IOException {
        // term1 [] term2/term3/term4 term5
        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 2, 3, 4),
            new Token("term3", 0, 3, 4),
            new Token("term4", 0, 3, 4),
            new Token("term5", 5, 6)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.ordered(
            Intervals.term("term1"),
            Intervals.extend(Intervals.or(Intervals.term("term2"), Intervals.term("term3"), Intervals.term("term4")), 1, 0),
            Intervals.term("term5")
        );
        assertEquals(expected, source);
    }

    public void testGraphSynonyms() throws IOException {

        // term1 term2:2/term3 term4 term5

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 1, 3, 4, 2),
            new Token("term3", 0, 3, 4),
            new Token("term4", 5, 6),
            new Token("term5", 6, 7)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(Intervals.term("term2"), Intervals.phrase("term3", "term4")),
            Intervals.term("term5")
        );

        assertEquals(expected, source);

    }

    public void testGraphSynonymsWithGaps() throws IOException {

        // term1 [] term2:4/term3 [] [] term4 term5

        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 2, 3, 4, 4),
            new Token("term3", 0, 3, 4),
            new Token("term4", 3, 5, 6),
            new Token("term5", 6, 7)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(
                Intervals.extend(Intervals.term("term2"), 1, 0),
                Intervals.phrase(
                    Intervals.extend(Intervals.term("term3"), 1, 0),
                    Intervals.extend(Intervals.term("term4"), 2, 0))),
            Intervals.term("term5")
        );

        assertEquals(expected, source);

    }

    public void testGraphTerminatesOnGap() throws IOException {
        // term1 term2:2/term3 term4 [] term5
        CannedTokenStream ts = new CannedTokenStream(
            new Token("term1", 1, 2),
            new Token("term2", 1, 2, 3, 2),
            new Token("term3", 0, 2, 3),
            new Token("term4", 2, 3),
            new Token("term5", 2, 6, 7)
        );

        IntervalsSource source = BUILDER.analyzeText(new CachingTokenFilter(ts), -1, true);
        IntervalsSource expected = Intervals.ordered(
            Intervals.term("term1"),
            Intervals.or(Intervals.term("term2"), Intervals.phrase("term3", "term4")),
            Intervals.extend(Intervals.term("term5"), 1, 0)
        );
        assertEquals(expected, source);
    }

}
