/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.codec.columnar.BehaviorDuelHarness;
import org.elasticsearch.test.codec.columnar.BehaviorWritePlan;
import org.elasticsearch.test.codec.columnar.DuelIndexAccess;
import org.elasticsearch.test.codec.columnar.KeywordIndexConfig;
import org.elasticsearch.test.codec.columnar.KeywordScenario;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Random;
import java.util.Set;

/**
 * ES|QL keyword behavior duel of a columnar-mode index with the ColumNAR codec disabled (the ES819 tsdb
 * doc-values format, {@code ES819Version3TSDBDocValuesFormat} at current index versions) against a ColumNAR
 * contender. Both indices use the same strict columnar index mode and differ only in the keyword doc-values
 * format, so the duel isolates the codec substitution as the single variable and compares API-visible ES|QL
 * behavior, not doc-values bytes. It runs every named corpus scenario through the shared
 * {@link BehaviorDuelHarness}. Query pragmas are pinned so both indices run under identical execution, and only
 * result rows are compared, never ES|QL warnings.
 */
// Force the production per-field codec so the contender's keyword field is actually encoded by ColumNAR.
// Without this, ESIntegTestCase sets index.codec=lucene_default, which bypasses the ColumNAR per-field
// routing, leaving the duel with nothing to compare.
@SuppressCodecs("*")
public class ES819KeywordEsqlBehaviorDuelIT extends AbstractEsqlIntegTestCase {

    private static final String DUEL_NAME = "es819_vs_columnar_esql";

    @Override
    protected Settings.Builder setRandomIndexSettings(final Random random, final Settings.Builder builder) {
        // Columnar modes require DOC_VALUES_ONLY for seq_no; remove the randomly chosen value so it does not
        // conflict with the mode's seq_no default.
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Pin execution so the baseline and contender indices are compared under identical strategy.
        return QueryPragmas.EMPTY;
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
            EsqlKeywordChecks.all()
        );
    }

    private IndexSettings indexSettings(final String indexName) {
        return DuelIndexAccess.indexSettings(internalCluster(), resolveIndex(indexName));
    }

    private Set<String> docValuesFormats(final String indexName) {
        indicesAdmin().prepareRefresh(indexName).get();
        return DuelIndexAccess.docValuesFormats(internalCluster(), resolveIndex(indexName), BehaviorDuelHarness.KEYWORD_FIELD);
    }
}
