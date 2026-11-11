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
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Runs one behavior duel pair: the same corpus is indexed, with the same write and merge plan, into a baseline
 * index and a contender index, then the {@link BehaviorCheck}s of every {@link DuelField} compare the baseline
 * against the contender. All failures across the checks are collected and reported together. The harness is shared
 * across field types and doc-values layouts: each {@link DuelField} carries its own mapping and checks, and each
 * {@link DuelIndexConfig} carries its own index mode, settings, expected doc-values format name, and
 * selector-guard flag, so another field type or doc-values codec is dueled by a child IT class without editing
 * the harness.
 *
 * <p>Before comparing, the harness asserts that, for every field, the distinct per-field doc-values formats
 * recorded across an index's started shard leaves are exactly its {@link DuelIndexConfig#expectedDocValuesFormatName()},
 * so a misconfigured contender cannot silently duel two identical layouts and pass without testing anything. A
 * layout that requests the ColumNAR selector guard also asserts the production selector routes the fields to
 * ColumNAR.
 *
 * <p>The baseline is also validated against an identically built copy of itself before it is compared to the
 * contender. A baseline-versus-baseline failure means the duel itself is not yet trustworthy, so it fails the
 * test directly rather than being reported as a contender mismatch.
 */
public final class BehaviorDuelHarness {

    /** The numeric identity field used as the sort tiebreak and retrieval anchor. */
    public static final String DOC_ID_FIELD = BehaviorWritePlan.DOC_ID_FIELD;

    /** The phase that compares the indices as the write plan left them. */
    public static final String INITIAL_PHASE = "initial";

    /** The phase that compares the indices again after the write plan's post-check force merge. */
    public static final String MERGED_PHASE = "merged";

    private static final Logger logger = LogManager.getLogger(BehaviorDuelHarness.class);

    private final Client client;
    private final Function<String, IndexSettings> indexSettingsResolver;
    private final BiFunction<String, String, Set<String>> docValuesFormatsResolver;

    /**
     * @param client                   the client to create, index, and query through
     * @param indexSettingsResolver    resolves a live index name to its {@link IndexSettings}, used to assert
     *                                 which doc-values format each index selects
     * @param docValuesFormatsResolver resolves a live index name and a field name to the distinct per-field
     *                                 doc-values format names written for that field across the index's started
     *                                 shard leaves, empty when no doc-values segment exists; used to assert the
     *                                 exact format engaged
     */
    public BehaviorDuelHarness(
        final Client client,
        final Function<String, IndexSettings> indexSettingsResolver,
        final BiFunction<String, String, Set<String>> docValuesFormatsResolver
    ) {
        this.client = client;
        this.indexSettingsResolver = indexSettingsResolver;
        this.docValuesFormatsResolver = docValuesFormatsResolver;
    }

    /**
     * Runs one duel pair: it indexes the same corpus with the same write plan into the baseline, an identical
     * copy of the baseline, and the contender. It validates the baseline against its copy, then runs every check
     * comparing the baseline to the contender. A plan with a post-check force merge repeats both steps after
     * the merge.
     *
     * @param duelName  the name of the duel pair, included in failure messages
     * @param scenario  the corpus scenario
     * @param plan      the write and merge plan applied identically to every index
     * @param baseline  the baseline index config
     * @param contender the contender index config
     * @param fields    the fields mapped in both indices, each with the checks that compare it
     */
    public void run(
        final String duelName,
        final DuelScenario scenario,
        final BehaviorWritePlan plan,
        final DuelIndexConfig baseline,
        final DuelIndexConfig contender,
        final List<DuelField> fields
    ) {
        final List<DuelDoc> docs = scenario.documents();
        final boolean corpusHasValues = docs.stream().anyMatch(doc -> doc.nonNullValues().isEmpty() == false);
        final DuelIndexConfig reference = baseline.withIndexName(baseline.indexName() + "_ref");

        try {
            createAndIndex(baseline, scenario, docs, plan, fields);
            createAndIndex(reference, scenario, docs, plan, fields);
            createAndIndex(contender, scenario, docs, plan, fields);
            assertDocValuesFormat(baseline, fields, corpusHasValues);
            assertDocValuesFormat(contender, fields, corpusHasValues);
            compare(INITIAL_PHASE, duelName, scenario, plan, docs, fields, baseline, reference, contender);

            if (plan.mergesAfterChecks()) {
                for (final DuelIndexConfig config : List.of(baseline, reference, contender)) {
                    plan.forceMergeAfterChecks(client, config.indexName());
                }
                assertDocValuesFormat(baseline, fields, corpusHasValues);
                assertDocValuesFormat(contender, fields, corpusHasValues);
                compare(MERGED_PHASE, duelName, scenario, plan, docs, fields, baseline, reference, contender);
            }
        } finally {
            deleteIndex(contender.indexName());
            deleteIndex(reference.indexName());
            deleteIndex(baseline.indexName());
        }
    }

    /**
     * Runs one comparison phase. It first proves the scenario, corpus, write plan, and checks are internally
     * valid by dueling the baseline against an identically built copy of itself. A baseline-versus-baseline
     * failure fails the test directly and is never classified as a ColumNAR mismatch, preserving the original
     * assertion as the cause. Only then does it duel the baseline against the contender and report every
     * mismatch together. The write plan is stateless (immutable batch and force-merge targets with a
     * deterministic split), so the same instance is reused for every index.
     */
    private void compare(
        final String phase,
        final String duelName,
        final DuelScenario scenario,
        final BehaviorWritePlan plan,
        final List<DuelDoc> docs,
        final List<DuelField> fields,
        final DuelIndexConfig baseline,
        final DuelIndexConfig reference,
        final DuelIndexConfig contender
    ) {
        for (final DuelField field : fields) {
            final DuelContext context = duelContext(duelName, phase, baseline, reference, scenario, plan, docs, field);
            for (final BehaviorCheck check : field.checks()) {
                try {
                    check.check(context);
                } catch (AssertionError | RuntimeException e) {
                    final String message = "LIKELY A TEST BUG (not a ColumNAR regression): baseline validation failed for duel ["
                        + duelName
                        + "], scenario ["
                        + scenario.name()
                        + "], baseline ["
                        + baseline.layoutLabel()
                        + "], field ["
                        + field.name()
                        + "], check ["
                        + check.name()
                        + "], write plan ["
                        + plan
                        + "], phase ["
                        + phase
                        + "]; the baseline disagrees with an identical copy of itself or with the corpus oracle, so this is"
                        + " most likely non-deterministic test behavior or a corpus/indexing/check/oracle bug rather than a"
                        + " ColumNAR contender mismatch";
                    logger.error(message, e);
                    throw new AssertionError(message, e);
                }
            }
        }

        final List<String> mismatches = new ArrayList<>();
        for (final DuelField field : fields) {
            final DuelContext context = duelContext(duelName, phase, baseline, contender, scenario, plan, docs, field);
            for (final BehaviorCheck check : field.checks()) {
                try {
                    check.check(context);
                } catch (AssertionError | RuntimeException e) {
                    mismatches.add(field.name() + "/" + check.name() + ": " + e.getMessage());
                }
            }
        }
        if (mismatches.isEmpty() == false) {
            throw new AssertionError(
                "behavior duel ["
                    + duelName
                    + "] failed for scenario ["
                    + scenario.name()
                    + "] phase ["
                    + phase
                    + "] baseline ["
                    + baseline.layoutLabel()
                    + "] contender ["
                    + contender.layoutLabel()
                    + "]:\n"
                    + String.join("\n", mismatches)
            );
        }
    }

    private DuelContext duelContext(
        final String duelName,
        final String phase,
        final DuelIndexConfig baseline,
        final DuelIndexConfig contender,
        final DuelScenario scenario,
        final BehaviorWritePlan plan,
        final List<DuelDoc> docs,
        final DuelField field
    ) {
        return new DuelContext(
            duelName,
            client,
            baseline.indexName(),
            contender.indexName(),
            baseline,
            contender,
            field.name(),
            DOC_ID_FIELD,
            scenario,
            plan,
            phase,
            docs
        );
    }

    private void createAndIndex(
        final DuelIndexConfig config,
        final DuelScenario scenario,
        final List<DuelDoc> docs,
        final BehaviorWritePlan plan,
        final List<DuelField> fields
    ) {
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(config.indexName())
                .setSettings(config.settings())
                .setMapping(mapping(fields, scenario.multiValue()))
        );
        plan.apply(client, config.indexName(), docs, fields.stream().map(DuelField::name).toList());
    }

    private void assertDocValuesFormat(final DuelIndexConfig config, final List<DuelField> fields, boolean corpusHasValues) {
        final IndexSettings indexSettings = indexSettingsResolver.apply(config.indexName());
        final boolean usesColumnar = ColumnarDocValuesFormatSelector.useColumnarCodec(indexSettings);

        for (final DuelField field : fields) {
            final Set<String> formats = docValuesFormatsResolver.apply(config.indexName(), field.name());

            // A ColumNAR-backed layout requests the selector guard, which asserts the production selector actually
            // routes the field to ColumNAR. Other codecs do not go through that selector, so it is not checked.
            if (config.enforcesColumnarSelectorGuard() && usesColumnar == false) {
                throw new AssertionError(
                    codecContext(config, field, indexSettings, usesColumnar, formats) + " expected the ColumNAR selector to engage"
                );
            }

            // Assert the distinct codecs recorded across the started leaves are exactly the expected one, so the duel
            // cannot pass with the wrong or a mixed codec engaged. An empty set means no doc-values segment was
            // written, which is only legitimate when the corpus holds no value.
            if (formats.isEmpty()) {
                if (corpusHasValues) {
                    throw new AssertionError(
                        codecContext(config, field, indexSettings, usesColumnar, formats)
                            + " wrote no doc-values segment for the field although the corpus holds values"
                    );
                }
                continue;
            }
            if (formats.equals(Set.of(config.expectedDocValuesFormatName())) == false) {
                throw new AssertionError(
                    codecContext(config, field, indexSettings, usesColumnar, formats) + " unexpected field doc-values format"
                );
            }
        }
    }

    private static String codecContext(
        final DuelIndexConfig config,
        final DuelField field,
        final IndexSettings indexSettings,
        boolean usesColumnar,
        final Set<String> formats
    ) {
        return "index ["
            + config.indexName()
            + "] field ["
            + field.name()
            + "] layout ["
            + config.layoutLabel()
            + "] mode ["
            + indexSettings.getMode()
            + "] index version created ["
            + indexSettings.getIndexVersionCreated()
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

    private static XContentBuilder mapping(final List<DuelField> fields, boolean multiValue) {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("properties");
            for (final DuelField field : fields) {
                field.mapping(builder, multiValue);
            }
            return builder.startObject(DOC_ID_FIELD)
                .field("type", "long")
                .endObject()
                .startObject(BehaviorWritePlan.TIMESTAMP_FIELD)
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
