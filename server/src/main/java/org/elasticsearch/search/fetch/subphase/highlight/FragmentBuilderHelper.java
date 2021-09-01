/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.index.analysis.AnalyzerComponentsProvider;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.util.List;

/**
 * Simple helper class for {@link FastVectorHighlighter} {@link FragmentsBuilder} implementations.
 */
public final class FragmentBuilderHelper {

    private FragmentBuilderHelper() {
      // no instance
    }

    /**
     * Fixes problems with broken analysis chains if positions and offsets are messed up that can lead to
     * {@link StringIndexOutOfBoundsException} in the {@link FastVectorHighlighter}
     */
    public static WeightedFragInfo fixWeightedFragInfo(WeightedFragInfo fragInfo) {
        assert fragInfo != null : "FragInfo must not be null";
        if (fragInfo.getSubInfos().isEmpty() == false) {
            /* This is a special case where broken analysis like WDF is used for term-vector creation at index-time
             * which can potentially mess up the offsets. To prevent a SAIIOBException we need to resort
             * the fragments based on their offsets rather than using solely the positions as it is done in
             * the FastVectorHighlighter. Yet, this is really a lucene problem and should be fixed in lucene rather
             * than in this hack... aka. "we are are working on in!" */
            final List<SubInfo> subInfos = fragInfo.getSubInfos();
            CollectionUtil.introSort(subInfos, (o1, o2) -> {
                int startOffset = o1.getTermsOffsets().get(0).getStartOffset();
                int startOffset2 = o2.getTermsOffsets().get(0).getStartOffset();
                return compare(startOffset, startOffset2);
            });
            return new WeightedFragInfo(Math.min(fragInfo.getSubInfos().get(0).getTermsOffsets().get(0).getStartOffset(),
                    fragInfo.getStartOffset()), fragInfo.getEndOffset(), subInfos, fragInfo.getTotalBoost());
        } else {
            return fragInfo;
        }
    }

    private static int compare(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    private static boolean containsBrokenAnalysis(Analyzer analyzer) {
        // TODO maybe we need a getter on Namedanalyzer that tells if this uses broken Analysis
        if (analyzer instanceof NamedAnalyzer) {
            analyzer = ((NamedAnalyzer) analyzer).analyzer();
        }
        if (analyzer instanceof AnalyzerComponentsProvider) {
            final TokenFilterFactory[] tokenFilters = ((AnalyzerComponentsProvider) analyzer).getComponents().getTokenFilters();
            for (TokenFilterFactory tokenFilterFactory : tokenFilters) {
                if (tokenFilterFactory.breaksFastVectorHighlighter()) {
                    return true;
                }
            }
        }
        return false;
    }
}
