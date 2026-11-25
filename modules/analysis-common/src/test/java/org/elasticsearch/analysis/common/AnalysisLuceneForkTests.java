/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.lucene.LuceneForks;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.lucene.LuceneForks.checkForksAreValid;
import static org.elasticsearch.lucene.LuceneForks.checkForksForExpiry;
import static org.elasticsearch.lucene.LuceneForks.compatibility;

public class AnalysisLuceneForkTests extends ESTestCase {

    public static final List<LuceneForks.Fork> ANALYSIS_FORKS = List.of(
        compatibility(
            "org.elasticsearch.analysis.common.XDictionaryCompoundWordTokenFilter",
            "Remove when index compatibility pre-Lucene 11 is not required"
        )
    );

    public void testForks() {
        checkForksAreValid(ANALYSIS_FORKS);
        checkForksForExpiry(ANALYSIS_FORKS);
    }
}
