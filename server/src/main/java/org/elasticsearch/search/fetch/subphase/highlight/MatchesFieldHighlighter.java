/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.highlight.OffsetLimitTokenFilter;
import org.apache.lucene.search.matchhighlight.OffsetRange;
import org.apache.lucene.search.matchhighlight.OffsetsFromTokens;
import org.apache.lucene.search.matchhighlight.OffsetsRetrievalStrategy;
import org.apache.lucene.search.matchhighlight.Passage;
import org.apache.lucene.search.matchhighlight.PassageFormatter;
import org.apache.lucene.search.matchhighlight.PassageSelector;
import org.apache.lucene.search.uhighlight.PassageScorer;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.TextSearchInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MatchesFieldHighlighter {

    private final FieldHighlightContext context;
    private final Matches matches;
    private final Analyzer analyzer;
    private final String field;

    public MatchesFieldHighlighter(FieldHighlightContext context, MatchesHighlighterState state) throws IOException {
        this.context = context;
        // TODO term vectors and require_field_match=false should intercept things here
        this.matches = state.getMatches(context.query, context.hitContext.docId());
        this.analyzer = context.context.getSearchExecutionContext().getIndexAnalyzer(s -> Lucene.STANDARD_ANALYZER);
        this.field = context.fieldType.name();
    }

    public MatchesIterator getMatchesIterator() throws IOException {
        if (this.matches == null) {
            return null;
        }
        MatchesIterator it = this.matches.getMatches(field);
        if (it == null || context.field.fieldOptions().maxAnalyzedOffset() == null) {
            return it;
        }
        int positionCutOff = context.field.fieldOptions().maxAnalyzedOffset() / 5;
        return new FilterMatchesIterator(it) {
            @Override
            public boolean next() throws IOException {
                if (it.next() == false) {
                    return false;
                }
                return it.startPosition() <= positionCutOff;
            }
        };
    }

    public List<String> buildHighlights(MatchesIterator it, List<CharSequence> sourceValues) throws IOException {
        String contiguousSourceText = buildContiguousSourceText(sourceValues);
        OffsetsRetrievalStrategy offsetsStrategy = getOffsetStrategy();
        List<OffsetRange> matchRanges = offsetsStrategy.get(it, f -> sourceValues);
        List<OffsetRange> sourceRanges = computeValueRanges(sourceValues);
        XPassageSelector passageSelector = new XPassageSelector();    // TODO word break stuff goes here
        PassageFormatter formatter = new PassageFormatter(
            "...",
            context.field.fieldOptions().preTags()[0],
            context.field.fieldOptions().postTags()[0]
        ); // TODO multiple field markers a la FVH
        List<Passage> passages =  passageSelector.pickBest(
            contiguousSourceText,
            matchRanges,
            context.field.fieldOptions().fragmentCharSize(),
            context.field.fieldOptions().numberOfFragments(),
            sourceRanges
        );
        return formatter.format(contiguousSourceText, passages, sourceRanges);
    }

    private OffsetsRetrievalStrategy getOffsetStrategy() {
        TextSearchInfo tsi = context.fieldType.getTextSearchInfo();
        // TODO termvectors
        return switch (tsi.luceneFieldType().indexOptions()) {
            case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS -> new XOffsetsFromMatchIterator(
                field,
                new XOffsetsFromPositions(field, analyzer)
            );
            case DOCS_AND_FREQS_AND_POSITIONS -> new XOffsetsFromPositions(field, analyzer);
            case DOCS_AND_FREQS, DOCS ->
                // By default retrieve offsets from individual tokens
                // retrieved by the analyzer (possibly narrowed down to
                // only those terms that the query hinted at when passed
                // a QueryVisitor.
                //
                // Alternative strategies are also possible and may make sense
                // depending on the use case (OffsetsFromValues, for example).
                new OffsetsFromTokens(field, analyzer);
            case NONE -> (matchesIterator, doc) -> {
                throw new IOException(
                    "Field is indexed without positions and/or offsets: "
                        + field
                        + ", "
                        + tsi.luceneFieldType().indexOptions());
            };
        };
    }

    private String buildContiguousSourceText(List<CharSequence> values) {
        String value;
        if (values.size() == 1) {
            value = values.get(0).toString();
        } else {
            // TODO: This can be inefficient if offset gap is large but the logic
            // of applying offsets would get much more complicated so leaving for now
            // (would have to recalculate all offsets to omit gaps).
            String fieldGapPadding = " ".repeat(analyzer.getOffsetGap(field));
            value = String.join(fieldGapPadding, values);
        }
        return value;
    }

    private List<OffsetRange> computeValueRanges(List<CharSequence> values) {
        ArrayList<OffsetRange> valueRanges = new ArrayList<>();
        int offset = 0;
        for (CharSequence v : values) {
            valueRanges.add(new OffsetRange(offset, offset + v.length()));
            offset += v.length();
            offset += analyzer.getOffsetGap(field);
        }
        return valueRanges;
    }
}
