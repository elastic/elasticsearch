/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks analyzer sharing end to end for analyzers {@link AnalysisFactoryTestCase}'s settings contract cannot reach: those whose
 * keys are built from files in the config directory, and the claim that sharing does not change output. An index that joins another
 * index's analyzer must tokenize exactly as it would with an analyzer of its own.
 */
public abstract class SharedAnalyzerTestCase extends ESTestCase {

    private Path configDir;
    private Environment environment;
    private final List<AnalysisRegistry> registries = new ArrayList<>();
    private final List<IndexAnalyzers> tracked = new ArrayList<>();

    /** The plugin whose analyzers are under test. */
    protected abstract AnalysisPlugin plugin();

    @Before
    public void setUpSharedAnalyzerTestCase() throws IOException {
        assumeTrue(
            "analyzer sharing feature flag disabled (release build); instance propagation is not observable",
            AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled()
        );
        Path home = createTempDir();
        configDir = Files.createDirectory(home.resolve("config"));
        environment = TestEnvironment.newEnvironment(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build());
    }

    @After
    public void closeSharedAnalyzerTestCase() throws IOException {
        IOUtils.close(tracked);
        for (AnalysisRegistry registry : registries) {
            registry.assertNoCachedEntries();
        }
        IOUtils.close(registries);
    }

    /** The node config directory, for files that analysis settings refer to by path. */
    protected Path configDir() {
        return configDir;
    }

    /** Index settings of the current version, plus {@code analysis}; analyzers under test are named {@code a}. */
    protected static Settings.Builder indexSettings(Settings analysis) {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put(analysis);
    }

    private AnalysisRegistry newRegistry() throws IOException {
        AnalysisRegistry registry = new AnalysisModule(environment, List.of(plugin()), new StablePluginsRegistry()).getAnalysisRegistry();
        registries.add(registry);
        return registry;
    }

    private Analyzer build(AnalysisRegistry registry, String index, Settings settings) throws IOException {
        IndexAnalyzers analyzers = registry.build(
            IndexService.IndexCreationContext.CREATE_INDEX,
            IndexSettingsModule.newIndexSettings(index, settings)
        );
        tracked.add(analyzers);
        return analyzers.get("a").analyzer();
    }

    /**
     * Asserts that index {@code b} built after index {@code a} joins {@code a}'s analyzer, and that the shared analyzer tokenizes
     * {@code text} exactly as an analyzer built for {@code b} alone does.
     */
    protected void assertSharesWithSameOutput(Settings a, Settings b, String text) throws IOException {
        AnalysisRegistry registry = newRegistry();
        Analyzer first = build(registry, "a", a);
        Analyzer joined = build(registry, "b", b);
        assertSame("index b must join index a's analyzer", first, joined);
        List<String> alone = terms(build(newRegistry(), "b", b), text);
        assertFalse("text must produce tokens", alone.isEmpty());
        assertEquals("the shared analyzer must tokenize as index b's own analyzer would", alone, terms(joined, text));
    }

    /** Asserts that index {@code b} built after index {@code a} gets an analyzer of its own. */
    protected void assertDoesNotShare(Settings a, Settings b) throws IOException {
        AnalysisRegistry registry = newRegistry();
        assertNotSame(build(registry, "a", a), build(registry, "b", b));
    }

    /**
     * One string per token carrying the term plus offsets, position increment/length and type, so
     * comparisons catch behavioral differences that leave the term text unchanged (e.g. the word
     * delimiter's {@code adjust_offsets}).
     */
    protected static List<String> terms(Analyzer analyzer, String text) throws IOException {
        List<String> terms = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream("f", text)) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
            PositionIncrementAttribute posInc = stream.addAttribute(PositionIncrementAttribute.class);
            PositionLengthAttribute posLen = stream.addAttribute(PositionLengthAttribute.class);
            TypeAttribute type = stream.addAttribute(TypeAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                terms.add(
                    Strings.format(
                        "%s [%d,%d) +%d/%d %s",
                        term,
                        offset.startOffset(),
                        offset.endOffset(),
                        posInc.getPositionIncrement(),
                        posLen.getPositionLength(),
                        type.type()
                    )
                );
            }
            stream.end();
        }
        return terms;
    }
}
