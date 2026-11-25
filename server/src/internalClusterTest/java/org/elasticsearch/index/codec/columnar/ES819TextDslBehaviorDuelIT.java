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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.codec.columnar.BehaviorCheck;
import org.elasticsearch.test.codec.columnar.BehaviorDuelHarness;
import org.elasticsearch.test.codec.columnar.BehaviorWritePlan;
import org.elasticsearch.test.codec.columnar.DuelField;
import org.elasticsearch.test.codec.columnar.DuelIndexAccess;
import org.elasticsearch.test.codec.columnar.DuelIndexConfig;
import org.elasticsearch.test.codec.columnar.DuelScenario;
import org.elasticsearch.test.codec.columnar.TextScenarios;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * DSL behavior duel of {@code text} and {@code match_only_text} fields in a columnar-mode index created just before
 * the ColumNAR codec (the ES819 tsdb doc-values format) against a ColumNAR contender. Both indices use the same strict
 * columnar index mode and differ only in the doc-values format, so the duel isolates the codec substitution as the
 * single variable. Each scenario runs in {@code columnar} and in {@code logsdb_columnar}, and each index pair maps
 * both types with the default {@code index} and with {@code index: false}. A text field is searched by the tokens of
 * its analyzed value either way, so all four fields run the same checks, once on the unmerged segments and again
 * after a force merge.
 */
// NOTE: force the production per-field codec so the contender's fields are actually encoded by ColumNAR.
// Without this, ESIntegTestCase sets index.codec=lucene_default, which bypasses the ColumNAR per-field
// routing, leaving the duel with nothing to compare.
@SuppressCodecs("*")
public class ES819TextDslBehaviorDuelIT extends ESIntegTestCase {

    private static final String DUEL_NAME = "es819_vs_columnar_text_dsl";
    private static final String TEXT_INDEXED = "text_indexed";
    private static final String TEXT_UNINDEXED = "text_unindexed";
    private static final String MATCH_ONLY_TEXT_INDEXED = "mot_indexed";
    private static final String MATCH_ONLY_TEXT_UNINDEXED = "mot_unindexed";

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // Need to set IndexVersion of baseline ES819 index.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder setRandomIndexSettings(final Random random, final Settings.Builder builder) {
        // NOTE: columnar modes require DOC_VALUES_ONLY for seq_no; remove the randomly chosen value so it does not
        // conflict with the mode's seq_no default.
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    public void testSentencesDense() {
        runDuel(TextScenarios.sentencesDense());
    }

    public void testSentencesSparse() {
        runDuel(TextScenarios.sentencesSparse());
    }

    public void testArrays() {
        runDuel(TextScenarios.arrays());
    }

    public void testUnicode() {
        runDuel(TextScenarios.unicode());
    }

    public void testLongValues() {
        runDuel(TextScenarios.longValues());
    }

    public void testEmptyStrings() {
        runDuel(TextScenarios.emptyStrings());
    }

    public void testLargeCorpus() {
        runDuel(TextScenarios.largeCorpus());
    }

    public void testSingleValued() {
        runDuel(TextScenarios.singleValued());
    }

    public void testRandomizedMixed() {
        runDuel(TextScenarios.randomizedMixed());
    }

    private void runDuel(final DuelScenario scenario) {
        final BehaviorDuelHarness harness = new BehaviorDuelHarness(client(), this::indexSettings, this::docValuesFormats);
        for (final IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            harness.run(
                DUEL_NAME,
                scenario,
                BehaviorWritePlan.multiSegmentCheckedBeforeAndAfterMerge(randomIntBetween(2, 4)),
                DuelIndexConfig.es819("text_baseline", mode),
                DuelIndexConfig.columnar("text_contender", mode),
                fields()
            );
        }
    }

    private static List<DuelField> fields() {
        return List.of(
            DuelField.text(TEXT_INDEXED, true, checks(true)),
            DuelField.text(TEXT_UNINDEXED, false, checks(true)),
            DuelField.matchOnlyText(MATCH_ONLY_TEXT_INDEXED, true, checks(true)),
            DuelField.matchOnlyText(MATCH_ONLY_TEXT_UNINDEXED, false, checks(false))
        );
    }

    private static List<BehaviorCheck> checks(boolean withIntervals) {
        final List<BehaviorCheck> checks = new ArrayList<>(DslKeywordSearchChecks.docValueChecks());
        checks.addAll(DslKeywordAnalyticsChecks.allExceptComposite());
        checks.addAll(DslTextChecks.checks());
        if (withIntervals) {
            checks.add(DslTextChecks.intervals());
        }
        return checks;
    }

    private IndexSettings indexSettings(final String indexName) {
        return DuelIndexAccess.indexSettings(internalCluster(), resolveIndex(indexName));
    }

    private Set<String> docValuesFormats(final String indexName, final String field) {
        indicesAdmin().prepareRefresh(indexName).get();
        return DuelIndexAccess.docValuesFormats(internalCluster(), resolveIndex(indexName), field);
    }
}
