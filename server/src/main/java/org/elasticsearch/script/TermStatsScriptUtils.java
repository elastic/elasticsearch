/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.search.TermStatistics;

import java.io.IOException;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.stream.DoubleStream;

public class TermStatsScriptUtils {

    public static final class DocumentFrequencyStatistics {
        private final List<TermStatistics> termsStatistics;

        public DocumentFrequencyStatistics(ScoreScript scoreScript, String fieldName, String query) throws IOException {
            this.termsStatistics = scoreScript._termsStatistics(fieldName, query);
        }

        public DoubleSummaryStatistics documentFrequencyStatistics() {
            return termsStatistics.stream().mapToDouble(termStats -> termStats == null ? 0 : termStats.docFreq()).summaryStatistics();
        }
    }

    public static final class TermFrequencyStatistics {
//        private final TermsStatsReader termsStatsReader;
//        private final Supplier<Integer> docIdSupplier;


        public TermFrequencyStatistics(ScoreScript scoreScript, String fieldName, String query) throws IOException {
//            this.termsStatsReader = scoreScript.docReader.termsStatsReader(fieldName, query);
//            this.docIdSupplier = scoreScript::_getDocId;
        }

        public DoubleSummaryStatistics termFrequencyStatistics() throws IOException {
            return DoubleStream.of(12).summaryStatistics();
        }
    }
}
