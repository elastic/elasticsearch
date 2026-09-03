/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.junit.Assume.assumeTrue;

/**
 * Runs one keyword behavior duel pair: the same corpus is indexed, with the same write and merge plan, into a
 * baseline index and a contender index, then a set of {@link BehaviorCheck}s compares the baseline against the
 * contender. All failures across the checks are collected and reported together. The harness is shared across
 * keyword doc-values layouts: each {@link KeywordIndexConfig} carries its own settings, expected doc-values
 * format name, and selector-guard flag, so another keyword doc-values codec is dueled by a child IT class that
 * returns a new contender layout, without editing the harness.
 *
 * <p>Before comparing, the harness asserts the distinct per-field doc-values formats recorded across an index's
 * started shard leaves are exactly its {@link KeywordIndexConfig#expectedDocValuesFormatName()}, so a
 * misconfigured contender cannot silently duel two identical layouts and pass without testing anything. A layout that requests
 * the ColumNAR selector guard
 * also asserts the production selector routes the keyword field to ColumNAR, and the pair is skipped when the
 * {@code columnar_codec} feature flag is disabled; a contender that does not request the guard is unaffected by
 * that flag.
 *
 * <p>The baseline is also validated against an identically built copy of itself before it is compared to the
 * contender. A baseline-versus-baseline failure means the duel itself is not yet trustworthy, so it fails the
 * test directly rather than being reported as a contender mismatch.
 */
public final class BehaviorDuelHarness {

    /** The keyword field under test. */
    public static final String KEYWORD_FIELD = "kw";

    /** The numeric identity field used as the sort tiebreak and retrieval anchor. */
    public static final String DOC_ID_FIELD = "doc_id";

    private static final Logger logger = LogManager.getLogger(BehaviorDuelHarness.class);

    private final Client client;
    private final Function<String, IndexSettings> indexSettingsResolver;
    private final Function<String, Set<String>> docValuesFormatsResolver;

    /**
     * @param client                   the client to create, index, and query through
     * @param indexSettingsResolver    resolves a live index name to its {@link IndexSettings}, used to assert
     *                                 which doc-values format each index selects
     * @param docValuesFormatsResolver resolves a live index name to the distinct per-field doc-values format
     *                                 names written for the field under test across its started shard leaves,
     *                                 empty when no doc-values segment exists; used to assert the exact format
     *                                 engaged
     */
    public BehaviorDuelHarness(
        final Client client,
        final Function<String, IndexSettings> indexSettingsResolver,
        final Function<String, Set<String>> docValuesFormatsResolver
    ) {
        this.client = client;
        this.indexSettingsResolver = indexSettingsResolver;
        this.docValuesFormatsResolver = docValuesFormatsResolver;
    }

    /**
     * Runs one duel pair: it validates the baseline against an identically built copy of itself, then indexes
     * the same corpus with the same write plan into the baseline and the contender and runs every check
     * comparing the baseline to the contender.
     *
     * @param duelName  the name of the duel pair, included in failure messages
     * @param scenario  the corpus scenario
     * @param plan      the write and merge plan applied identically to every index
     * @param baseline  the baseline index config
     * @param contender the contender index config
     * @param checks    the behavior checks to run
     */
    public void run(
        final String duelName,
        final KeywordScenario scenario,
        final BehaviorWritePlan plan,
        final KeywordIndexConfig baseline,
        final KeywordIndexConfig contender,
        final List<BehaviorCheck> checks
    ) {
        // Only a ColumNAR-backed contender depends on the feature flag; a future codec that does not request the
        // selector guard must not be skipped by it.
        if (contender.enforcesColumnarSelectorGuard()) {
            assumeTrue(
                "columnar_codec feature flag must be enabled",
                ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled()
            );
        }

        final List<KeywordDoc> docs = scenario.documents();
        final boolean corpusHasKeywordValues = docs.stream().anyMatch(doc -> doc.nonNullValues().isEmpty() == false);

        try {
            createAndIndex(baseline, docs, plan);
            assertDocValuesFormat(baseline, corpusHasKeywordValues);
            assertBaselineValid(duelName, scenario, baseline, plan, docs, checks);

            try {
                createAndIndex(contender, docs, plan);
                assertDocValuesFormat(contender, corpusHasKeywordValues);

                final DuelContext context = duelContext(duelName, baseline, contender, scenario, plan, docs);
                final List<String> mismatches = new ArrayList<>();
                for (final BehaviorCheck check : checks) {
                    try {
                        check.check(context);
                    } catch (AssertionError e) {
                        mismatches.add(check.name() + ": " + e.getMessage());
                    }
                }
                if (mismatches.isEmpty() == false) {
                    throw new AssertionError(
                        "keyword behavior duel ["
                            + duelName
                            + "] failed for scenario ["
                            + scenario.name()
                            + "] baseline ["
                            + baseline.layoutLabel()
                            + "] contender ["
                            + contender.layoutLabel()
                            + "]:\n"
                            + String.join("\n", mismatches)
                    );
                }
            } finally {
                deleteIndex(contender.indexName());
            }
        } finally {
            deleteIndex(baseline.indexName());
        }
    }

    /**
     * Proves the scenario, corpus, write plan, and checks are internally valid by dueling the baseline against
     * an identically built copy of itself before it is compared to the contender. A baseline-versus-baseline
     * failure fails the test directly and is never classified as a ColumNAR mismatch, preserving the original
     * assertion as the cause. The write plan is stateless (immutable batch and force-merge targets with a
     * deterministic split), so the same instance is reused for both baseline copies.
     */
    private void assertBaselineValid(
        final String duelName,
        final KeywordScenario scenario,
        final KeywordIndexConfig baseline,
        final BehaviorWritePlan plan,
        final List<KeywordDoc> docs,
        final List<BehaviorCheck> checks
    ) {
        final KeywordIndexConfig reference = baseline.withIndexName(baseline.indexName() + "_ref");
        try {
            createAndIndex(reference, docs, plan);
            final DuelContext context = duelContext(duelName, baseline, reference, scenario, plan, docs);
            for (final BehaviorCheck check : checks) {
                try {
                    check.check(context);
                } catch (AssertionError e) {
                    final String message = "LIKELY A TEST BUG (not a ColumNAR regression): baseline validation failed for duel ["
                        + duelName
                        + "], scenario ["
                        + scenario.name()
                        + "], baseline ["
                        + baseline.layoutLabel()
                        + "], check ["
                        + check.name()
                        + "], write plan ["
                        + plan
                        + "]; the baseline disagrees with an identical copy of itself or with the corpus oracle, so this is"
                        + " most likely non-deterministic test behavior or a corpus/indexing/check/oracle bug rather than a"
                        + " ColumNAR contender mismatch";
                    logger.error(message, e);
                    throw new AssertionError(message, e);
                }
            }
        } finally {
            deleteIndex(reference.indexName());
        }
    }

    private DuelContext duelContext(
        final String duelName,
        final KeywordIndexConfig baseline,
        final KeywordIndexConfig contender,
        final KeywordScenario scenario,
        final BehaviorWritePlan plan,
        final List<KeywordDoc> docs
    ) {
        return new DuelContext(
            duelName,
            client,
            baseline.indexName(),
            contender.indexName(),
            baseline,
            contender,
            KEYWORD_FIELD,
            DOC_ID_FIELD,
            scenario,
            plan,
            docs
        );
    }

    private void createAndIndex(final KeywordIndexConfig config, final List<KeywordDoc> docs, final BehaviorWritePlan plan) {
        assertAcked(client.admin().indices().prepareCreate(config.indexName()).setSettings(config.settings()).setMapping(mapping()));
        plan.apply(client, config.indexName(), docs, KEYWORD_FIELD);
    }

    private void assertDocValuesFormat(final KeywordIndexConfig config, boolean corpusHasKeywordValues) {
        final IndexSettings indexSettings = indexSettingsResolver.apply(config.indexName());
        final boolean usesColumnar = ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings);
        final Set<String> formats = docValuesFormatsResolver.apply(config.indexName());

        // A ColumNAR-backed layout requests the selector guard, which asserts the production selector actually
        // routes the keyword field to ColumNAR. Other codecs do not go through that selector, so it is not checked.
        if (config.enforcesColumnarSelectorGuard() && usesColumnar == false) {
            throw new AssertionError(
                codecContext(config, indexSettings, usesColumnar, formats) + " expected the ColumNAR selector to engage"
            );
        }

        // Assert the distinct codecs recorded across the started leaves are exactly the expected one, so the duel
        // cannot pass with the wrong or a mixed codec engaged. An empty set means no doc-values segment was
        // written, which is only legitimate when the corpus holds no keyword value.
        if (formats.isEmpty()) {
            if (corpusHasKeywordValues) {
                throw new AssertionError(
                    codecContext(config, indexSettings, usesColumnar, formats)
                        + " wrote no doc-values segment for the field although the corpus holds keyword values"
                );
            }
            return;
        }
        if (formats.equals(Set.of(config.expectedDocValuesFormatName())) == false) {
            throw new AssertionError(codecContext(config, indexSettings, usesColumnar, formats) + " unexpected field doc-values format");
        }
    }

    private static String codecContext(
        final KeywordIndexConfig config,
        final IndexSettings indexSettings,
        boolean usesColumnar,
        final Set<String> formats
    ) {
        return "index ["
            + config.indexName()
            + "] layout ["
            + config.layoutLabel()
            + "] mode ["
            + indexSettings.getMode()
            + "] index.columnar_codec.enabled ["
            + indexSettings.getSettings().getAsBoolean(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), false)
            + "] useColumnarCodec ["
            + usesColumnar
            + "] expected format ["
            + config.expectedDocValuesFormatName()
            + "] actual formats "
            + formats;
    }

    private void deleteIndex(final String indexName) {
        assertAcked(client.admin().indices().prepareDelete(indexName).setIndicesOptions(IndicesOptions.lenientExpandOpen()));
    }

    private static XContentBuilder mapping() {
        try {
            return XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(KEYWORD_FIELD)
                .field("type", "keyword")
                .endObject()
                .startObject(DOC_ID_FIELD)
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
