/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Analyzer-sharing cases in analysis-common that {@code CommonAnalysisFactoryTests}' settings contract
 * cannot express: keys that depend on the index created version, and per-index validation that must
 * still run when a sibling index already cached the same recipe. Builds real {@link IndexAnalyzers}
 * through {@link AnalysisRegistry}, the path index creation takes.
 */
public class SharingKeyTests extends ESTestCase {

    private AnalysisRegistry registry;
    private final List<IndexAnalyzers> tracked = new ArrayList<>();

    @Before
    public void setUpRegistry() throws IOException {
        assumeTrue(
            "analyzer sharing feature flag disabled (release build); instance propagation is not observable",
            AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled()
        );
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        registry = new AnalysisModule(
            TestEnvironment.newEnvironment(nodeSettings),
            List.of(new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    @After
    public void closeAndAssertNoLeaks() throws IOException {
        if (registry == null) {
            return;
        }
        IOUtils.close(tracked);
        registry.assertNoCachedEntries();
        registry.close();
    }

    private Analyzer build(IndexVersion version, Settings analysis) throws IOException {
        Settings s = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).put(analysis).build();
        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("test", s));
        tracked.add(ia);
        // The per-index NamedAnalyzer wrapper is allocated per build, so compare the wrapped analyzer.
        return ia.get("a").analyzer();
    }

    /** Before Lucene 10 the romanian analyzer keeps cedilla forms, so the version fork must be part of the key. */
    public void testRomanianAnalyzerDoesNotShareAcrossVersions() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
        assumeTrue("no read-compatible version predates Lucene 10", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        Settings romanian = Settings.builder().put("index.analysis.analyzer.a.type", "romanian").build();

        Analyzer current = build(IndexVersion.current(), romanian);
        assertSame(current, build(IndexVersion.current(), romanian));
        assertNotSame("romanian analyzer behavior differs by index version and must not share", current, build(legacy, romanian));
    }

    /** The unique filter only corrects position increments on indices created after the fix, so that must be part of the key. */
    public void testUniqueFilterDoesNotShareAcrossPositionFixVersion() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.UNIQUE_TOKEN_FILTER_POS_FIX);
        assumeTrue("no read-compatible version predates the fix", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));
        Settings unique = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "u")
            .put("index.analysis.filter.u.type", "unique")
            .build();

        Analyzer current = build(IndexVersion.current(), unique);
        assertSame(current, build(IndexVersion.current(), unique));
        assertNotSame("unique filter behavior differs by index version and must not share", current, build(legacy, unique));
    }

    /**
     * Per-index validation (here {@code index.max_ngram_diff}) must fire even when another index has
     * already cached an identical chain: the check runs in the factory constructor, which is called
     * per index before the analyzer cache is consulted.
     */
    public void testPerIndexNgramDiffValidationFiresEvenWithCachedSibling() throws IOException {
        Settings recipe = Settings.builder()
            .put("index.analysis.filter.ng.type", "ngram")
            .put("index.analysis.filter.ng.min_gram", 1)
            .put("index.analysis.filter.ng.max_gram", 5)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "ng")
            .build();
        build(IndexVersion.current(), Settings.builder().put("index.max_ngram_diff", 10).put(recipe).build());

        Settings strict = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.max_ngram_diff", 1)
            .put(recipe)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("b", strict))
        );
        assertThat(e.getMessage(), containsString("max_gram and min_gram"));
    }
}
