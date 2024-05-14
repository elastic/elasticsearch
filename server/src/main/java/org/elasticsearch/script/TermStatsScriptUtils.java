/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.TermStatistics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.function.Supplier;

;

public class TermStatsScriptUtils {

    public static final class DocumentFrequencyStatistics {
        private final Collection<TermStatistics> termsStatistics;

        public DocumentFrequencyStatistics(ScoreScript scoreScript) {
            this.termsStatistics = scoreScript._termStatistics().values();
        }

        public DoubleSummaryStatistics documentFrequencyStatistics() {
            return termsStatistics.stream().mapToDouble(termStatistics -> termStatistics == null ? 0 : termStatistics.docFreq()).summaryStatistics();
        }
    }

    public static final class TermFrequencyStatistics {
        private final Collection<PostingsEnum> postings;
        private final Supplier<Integer> docIdSupplier;

        public TermFrequencyStatistics(ScoreScript scoreScript) {
            postings = scoreScript._postings(PostingsEnum.FREQS).values();
            docIdSupplier = scoreScript::_getDocId;
        }

        public DoubleSummaryStatistics termFrequencyStatistics() {
            return postings.stream().mapToDouble(
                currentPostings -> {
                    try {
                        int docId = docIdSupplier.get();
                        if (currentPostings == null || currentPostings.advance(docId) != docId) {
                            return 0;
                        }
                        return currentPostings.freq();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            ).summaryStatistics();
        }
    }
}
