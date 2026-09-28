/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.codec.columnar.BehaviorCheck;
import org.elasticsearch.test.codec.columnar.BehaviorDuelHarness;
import org.elasticsearch.test.codec.columnar.BehaviorWritePlan;
import org.elasticsearch.test.codec.columnar.DuelIndexAccess;
import org.elasticsearch.test.codec.columnar.KeywordIndexConfig;
import org.elasticsearch.test.codec.columnar.KeywordScenario;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * DSL keyword behavior duel of a columnar-mode index with the ColumNAR codec disabled (the ES819 tsdb
 * doc-values format, {@code ES819Version3TSDBDocValuesFormat} at current index versions) against a ColumNAR
 * contender. Both indices use the same strict columnar index mode and differ only in the keyword doc-values
 * format, so the duel isolates the codec substitution as the single variable and compares API-visible DSL
 * behavior, not doc-values bytes. It runs every named corpus scenario through the shared
 * {@link BehaviorDuelHarness}.
 */
// Force the production per-field codec so the contender's keyword field is actually encoded by ColumNAR.
// Without this, ESIntegTestCase sets index.codec=lucene_default, which bypasses the ColumNAR per-field
// routing, leaving the duel with nothing to compare.
@SuppressCodecs("*")
public class ES819KeywordDslBehaviorDuelIT extends ESIntegTestCase {

    private static final String DUEL_NAME = "es819_vs_columnar_dsl";

    @Override
    protected Settings.Builder setRandomIndexSettings(final Random random, final Settings.Builder builder) {
        // Columnar modes require DOC_VALUES_ONLY for seq_no; remove the randomly chosen value so it does not
        // conflict with the mode's seq_no default.
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    public void testSingleDense() {
        runDuel(KeywordScenario.singleDense());
    }

    public void testSingleSparse() {
        runDuel(KeywordScenario.singleSparse());
    }

    public void testMultiDense() {
        runDuel(KeywordScenario.multiDense());
    }

    public void testMultiSparse() {
        runDuel(KeywordScenario.multiSparse());
    }

    public void testNulls() {
        runDuel(KeywordScenario.nulls());
    }

    public void testEmptyArrays() {
        runDuel(KeywordScenario.emptyArrays());
    }

    public void testDuplicates() {
        runDuel(KeywordScenario.duplicates());
    }

    public void testHighCardinality() {
        runDuel(KeywordScenario.highCardinality());
    }

    public void testUnicode() {
        runDuel(KeywordScenario.unicode());
    }

    public void testLongValues() {
        runDuel(KeywordScenario.longValues());
    }

    public void testRandomizedMixed() {
        runDuel(KeywordScenario.randomizedMixed());
    }

    public void testEmptyStrings() {
        runDuel(KeywordScenario.emptyStrings());
    }

    public void testLargeCorpus() {
        runDuel(KeywordScenario.largeCorpus());
    }

    private void runDuel(final KeywordScenario scenario) {
        final BehaviorDuelHarness harness = new BehaviorDuelHarness(client(), this::indexSettings, this::docValuesFormats);
        harness.run(
            DUEL_NAME,
            scenario,
            BehaviorWritePlan.random(),
            KeywordIndexConfig.es819("kw_baseline"),
            KeywordIndexConfig.columnar("kw_contender"),
            checks()
        );
    }

    private static List<BehaviorCheck> checks() {
        final List<BehaviorCheck> checks = new ArrayList<>(DslKeywordSearchChecks.all());
        checks.addAll(DslKeywordAnalyticsChecks.all());
        return checks;
    }

    private IndexSettings indexSettings(final String indexName) {
        return DuelIndexAccess.indexSettings(internalCluster(), resolveIndex(indexName));
    }

    private Set<String> docValuesFormats(final String indexName) {
        indicesAdmin().prepareRefresh(indexName).get();
        return DuelIndexAccess.docValuesFormats(internalCluster(), resolveIndex(indexName), BehaviorDuelHarness.KEYWORD_FIELD);
    }
}
