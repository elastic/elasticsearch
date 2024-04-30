/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.io.IOException;
import java.util.DoubleSummaryStatistics;
import java.util.function.Supplier;

public class TermStatsScriptUtils {

    public static final class DocumentFrequencyStatistics {
        private final TermsStatsReader termsStatsReader;

        public DocumentFrequencyStatistics(ScoreScript scoreScript, String fieldName, String query) throws IOException {
            this.termsStatsReader = scoreScript.docReader.termsStatsReader(fieldName, query);
        }

        public DoubleSummaryStatistics documentFrequencyStatistics() {
            return termsStatsReader.docFrequencies().values().stream().mapToDouble(Integer::doubleValue).summaryStatistics();
        }
    }

    public static final class TermFrequencyStatistics {
        private final TermsStatsReader termsStatsReader;
        private final Supplier<Integer> docIdSupplier;


        public TermFrequencyStatistics(ScoreScript scoreScript, String fieldName, String query) throws IOException {
            this.termsStatsReader = scoreScript.docReader.termsStatsReader(fieldName, query);
            this.docIdSupplier = scoreScript::_getDocId;
        }

        public DoubleSummaryStatistics termFrequencyStatistics() throws IOException {
            return termsStatsReader.termFrequencies(docIdSupplier.get()).values().stream().mapToDouble(Integer::doubleValue).summaryStatistics();
        }
    }
}
