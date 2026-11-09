/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.phonetic;

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

/**
 * The legacy/fixed haasephonetik split is derived from the index version, not a setting, so it must be part of the
 * sharing key. Builds real {@link IndexAnalyzers} through {@link AnalysisRegistry}, the path index creation takes.
 */
public class HaasePhonetikSharingTests extends ESTestCase {

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
            List.of(new AnalysisPhoneticPlugin()),
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

    private Analyzer build(IndexVersion version, String encoder) throws IOException {
        Settings s = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "p")
            .put("index.analysis.filter.p.type", "phonetic")
            .put("index.analysis.filter.p.encoder", encoder)
            .build();
        IndexAnalyzers ia = registry.build(IndexService.IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("test", s));
        tracked.add(ia);
        // The per-index NamedAnalyzer wrapper is allocated per build, so compare the wrapped analyzer.
        return ia.get("a").analyzer();
    }

    public void testHaasePhonetikDoesNotShareAcrossFixVersion() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.HAASE_PHONETIK_ANCHORED_VARIATIONS);
        assumeTrue("no read-compatible version predates the fix", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));

        Analyzer current = build(IndexVersion.current(), "haasephonetik");
        assertSame(current, build(IndexVersion.current(), "haasephonetik"));
        assertNotSame("haasephonetik codes differ by index version and must not share", current, build(legacy, "haasephonetik"));
    }

    public void testOtherEncodersStillShareAcrossFixVersion() throws IOException {
        IndexVersion legacy = IndexVersionUtils.getPreviousVersion(IndexVersions.HAASE_PHONETIK_ANCHORED_VARIATIONS);
        assumeTrue("no read-compatible version predates the fix", legacy.onOrAfter(IndexVersionUtils.getLowestReadCompatibleVersion()));

        assertSame(build(IndexVersion.current(), "koelnerphonetik"), build(legacy, "koelnerphonetik"));
    }
}
