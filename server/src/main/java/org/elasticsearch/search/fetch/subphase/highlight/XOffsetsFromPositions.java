/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.matchhighlight.MatchRegionRetriever;
import org.apache.lucene.search.matchhighlight.OffsetRange;
import org.apache.lucene.search.matchhighlight.OffsetsRetrievalStrategy;
import org.elasticsearch.Version;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class XOffsetsFromPositions implements OffsetsRetrievalStrategy {
    private final String field;
    private final Analyzer analyzer;

    public XOffsetsFromPositions(String field, Analyzer analyzer) {
        this.field = field;
        this.analyzer = analyzer;
        // https://github.com/apache/lucene/pull/11983
        assert Version.CURRENT.luceneVersion.onOrAfter(org.apache.lucene.util.Version.fromBits(9, 5, 0)) == false;
    }

    @Override
    public List<OffsetRange> get(MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc) throws IOException {
        ArrayList<OffsetRange> positionRanges = new ArrayList<>();
        while (matchesIterator.next()) {
            int from = matchesIterator.startPosition();
            int to = matchesIterator.endPosition();
            if (from < 0 || to < 0) {
                throw new IOException("Matches API returned negative positions for field: " + field);
            }
            positionRanges.add(new OffsetRange(from, to));
        }

        // Convert from positions to offsets.
        return convertPositionsToOffsets(positionRanges, doc.getValues(field));
    }

    List<OffsetRange> convertPositionsToOffsets(ArrayList<OffsetRange> positionRanges, List<CharSequence> values) throws IOException {

        if (positionRanges.isEmpty()) {
            return positionRanges;
        }

        class PositionSpan extends OffsetRange {
            int leftOffset = Integer.MAX_VALUE;
            int rightOffset = Integer.MIN_VALUE;

            PositionSpan(int from, int to) {
                super(from, to);
            }

            @Override
            public String toString() {
                return "[from=" + from + ", to=" + to + ", L: " + leftOffset + ", R: " + rightOffset + ']';
            }
        }

        List<PositionSpan> spans = new ArrayList<>();
        int minPosition = Integer.MAX_VALUE;
        int maxPosition = Integer.MIN_VALUE;
        for (OffsetRange range : positionRanges) {
            spans.add(new PositionSpan(range.from, range.to));
            minPosition = Math.min(minPosition, range.from);
            maxPosition = Math.max(maxPosition, range.to);
        }

        PositionSpan[] spansTable = spans.toArray(PositionSpan[]::new);
        int spanCount = spansTable.length;
        int position = -1;
        int valueOffset = 0;
        int convertedMatchCount = 0;
        for (int valueIndex = 0, max = values.size(); valueIndex < max; valueIndex++) {
            final String value = values.get(valueIndex).toString();
            final boolean lastValue = valueIndex + 1 == max;

            TokenStream ts = analyzer.tokenStream(field, value);
            OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
            PositionIncrementAttribute posAttr = ts.getAttribute(PositionIncrementAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                position += posAttr.getPositionIncrement();

                if (position >= minPosition) {
                    // Correct left and right offsets for each span this position applies to.
                    int startOffset = valueOffset + offsetAttr.startOffset();
                    int endOffset = valueOffset + offsetAttr.endOffset();

                    int j = 0;
                    for (int i = 0; i < spanCount; i++) {
                        PositionSpan span = spansTable[j] = spansTable[i];
                        if (position >= span.from) {
                            if (position <= span.to) {
                                span.leftOffset = Math.min(span.leftOffset, startOffset);
                                span.rightOffset = Math.max(span.rightOffset, endOffset);
                                convertedMatchCount++;
                            } else {
                                // this span can't intersect with any following position
                                // so omit it by skipping j++.
                                continue;
                            }
                        }
                        j++;
                    }
                    spanCount = j;

                    // Only short-circuit if we're on the last value (which should be the common
                    // case since most fields would only have a single value anyway). We need
                    // to make sure of this because otherwise offsetAttr would have incorrect value.
                    if (position > maxPosition && lastValue) {
                        break;
                    }
                }
            }
            ts.end();
            position += posAttr.getPositionIncrement() + analyzer.getPositionIncrementGap(field);
            valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(field);
            ts.close();
        }

        if (convertedMatchCount == 0) {
            return Collections.emptyList();
        }
        spans = spans.subList(0, convertedMatchCount);   // remove any matches beyond the max analyzed offset limit
        ArrayList<OffsetRange> converted = new ArrayList<>(spans.size());
        for (PositionSpan span : spans) {
            if (span.leftOffset == Integer.MAX_VALUE || span.rightOffset == Integer.MIN_VALUE) {
                throw new RuntimeException("One of the offsets missing for position range: " + span);
            }
            converted.add(new OffsetRange(span.leftOffset, span.rightOffset));
        }
        return converted;
    }
}
