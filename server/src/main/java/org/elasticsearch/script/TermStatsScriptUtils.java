/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.DoubleSummaryStatistics;
import java.util.stream.DoubleStream;

public class TermStatsScriptUtils {

    public static final class DocumentFrequencyStatistics {
        public DocumentFrequencyStatistics(ScoreScript scoreScript, String fieldName, String query) {

        }

        public DoubleSummaryStatistics documentFrequencyStatistics() {
            return DoubleStream.empty().summaryStatistics();
        }

    }
}
