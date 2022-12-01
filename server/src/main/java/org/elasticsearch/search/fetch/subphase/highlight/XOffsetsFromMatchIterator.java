/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;


import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.matchhighlight.MatchRegionRetriever;
import org.apache.lucene.search.matchhighlight.OffsetRange;
import org.apache.lucene.search.matchhighlight.OffsetsFromPositions;
import org.apache.lucene.search.matchhighlight.OffsetsRetrievalStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This strategy retrieves offsets directly from {@link MatchesIterator}, if they are available,
 * otherwise it falls back to using {@link OffsetsFromPositions}.
 */
// https://github.com/apache/lucene/pull/11983
public final class XOffsetsFromMatchIterator implements OffsetsRetrievalStrategy {
    private final String field;
    private final XOffsetsFromPositions noOffsetsFallback;

    XOffsetsFromMatchIterator(String field, XOffsetsFromPositions noOffsetsFallback) {
        this.field = field;
        this.noOffsetsFallback = Objects.requireNonNull(noOffsetsFallback);
    }

    @Override
    public List<OffsetRange> get(
        MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
        throws IOException {
        ArrayList<OffsetRange> positionRanges = new ArrayList<>();
        ArrayList<OffsetRange> offsetRanges = new ArrayList<>();
        while (matchesIterator.next()) {
            int fromPosition = matchesIterator.startPosition();
            int toPosition = matchesIterator.endPosition();
            if (fromPosition < 0 || toPosition < 0) {
                throw new IOException("Matches API returned negative positions for field: " + field);
            }
            positionRanges.add(new OffsetRange(fromPosition, toPosition));

            if (offsetRanges != null) {
                int from = matchesIterator.startOffset();
                int to = matchesIterator.endOffset();
                if (from < 0 || to < 0) {
                    // At least one offset isn't available. Fallback to just positions.
                    offsetRanges = null;
                } else {
                    offsetRanges.add(new OffsetRange(from, to));
                }
            }
        }

        // Use the fallback conversion from positions if not all offsets were available.
        if (offsetRanges == null) {
            return noOffsetsFallback.convertPositionsToOffsets(positionRanges, doc.getValues(field));
        } else {
            return offsetRanges;
        }
    }
}
