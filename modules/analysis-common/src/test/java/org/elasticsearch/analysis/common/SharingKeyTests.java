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
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;

/**
 * Analyzer-sharing cases in analysis-common that {@code CommonAnalysisFactoryTests}' settings contract
 * cannot express: keys that depend on the index created version, per-index validation that must
 * still run when a sibling index already cached the same recipe, and the synonym filters, which need
 * the plugin's services. Builds real {@link IndexAnalyzers} through {@link AnalysisRegistry}, the path
 * index creation takes.
 */
public class SharingKeyTests extends ESTestCase {

    private TestThreadPool threadPool;
    private AnalysisRegistry registry;
    private Environment environment;
    private String synonymType;
    private final List<IndexAnalyzers> tracked = new ArrayList<>();

    @Before
    public void setUpRegistry() throws IOException {
        assumeTrue(
            "analyzer sharing feature flag disabled (release build); instance propagation is not observable",
            AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled()
        );
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        environment = TestEnvironment.newEnvironment(nodeSettings);
        synonymType = randomFrom("synonym", "synonym_graph");
        Files.createDirectories(environment.configDir());
        threadPool = new TestThreadPool(getTestName());
        CommonAnalysisPlugin plugin = new TestCommonAnalysisPluginBuilder(threadPool).build();
        registry = new AnalysisModule(environment, List.of(plugin), new StablePluginsRegistry()).getAnalysisRegistry();
    }

    @After
    public void closeAndAssertNoLeaks() throws IOException {
        if (registry == null) {
            return;
        }
        try {
            IOUtils.close(tracked);
            registry.assertNoCachedEntries();
            registry.close();
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
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

    private Settings synonymChain(String source, Object value, boolean updateable) {
        Settings.Builder b = Settings.builder()
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "syn")
            .put("index.analysis.filter.syn.type", synonymType)
            .put("index.analysis.filter.syn.updateable", updateable);
        if (value instanceof List<?> list) {
            b.putList("index.analysis.filter.syn." + source, list.stream().map(Object::toString).toList());
        } else {
            b.put("index.analysis.filter.syn." + source, value.toString());
        }
        return b.build();
    }

    private void writeConfigFile(String name, String content) throws IOException {
        Files.writeString(environment.configDir().resolve(name), content);
    }

    /** Every inline-rule setting the key reads must split the instance; {@code format} only matters when it is wordnet. */
    public void testInlineSynonymsKey() throws IOException {
        List<String> rules = List.of("a, b", "c, d");
        Analyzer base = build(IndexVersion.current(), synonymChain("synonyms", rules, false));
        assertSame(base, build(IndexVersion.current(), synonymChain("synonyms", rules, false)));
        // The key keeps the rule list as written.
        assertNotSame(base, build(IndexVersion.current(), synonymChain("synonyms", List.of("c, d", "a, b"), false)));
        assertNotSame(base, build(IndexVersion.current(), synonymChain("synonyms", List.of("a, b"), false)));
        assertNotSame(base, build(IndexVersion.current(), synonymChain("synonyms", rules, true)));
        for (String setting : List.of("expand=false", "lenient=true")) {
            String[] kv = setting.split("=");
            Settings varied = Settings.builder()
                .put(synonymChain("synonyms", rules, false))
                .put("index.analysis.filter.syn." + kv[0], kv[1])
                .build();
            assertNotSame(setting, base, build(IndexVersion.current(), varied));
        }
        Settings solr = Settings.builder()
            .put(synonymChain("synonyms", rules, false))
            .put("index.analysis.filter.syn.format", "solr")
            .build();
        assertSame("solr is the default format", base, build(IndexVersion.current(), solr));
    }

    /** The rules are fetched in one query sorted by rule id, so neither set order nor repeats change them. */
    public void testSynonymsSetKeyIgnoresOrderAndRepeats() throws IOException {
        Analyzer s1s2 = build(IndexVersion.current(), synonymChain("synonyms_set", List.of("s1", "s2"), true));
        assertSame(s1s2, build(IndexVersion.current(), synonymChain("synonyms_set", List.of("s2", "s1", "s2"), true)));
        assertNotSame(s1s2, build(IndexVersion.current(), synonymChain("synonyms_set", List.of("s1"), true)));
    }

    /** Non-updateable file rules are read once at index creation, so the key is their content, not their path. */
    public void testNonUpdateableSynonymsFileKeysOnContent() throws IOException {
        writeConfigFile("one.txt", "a, b");
        writeConfigFile("two.txt", "a, b");
        Analyzer one = build(IndexVersion.current(), synonymChain("synonyms_path", "one.txt", false));
        assertSame("same rules under another path", one, build(IndexVersion.current(), synonymChain("synonyms_path", "two.txt", false)));

        writeConfigFile("one.txt", "a, c");
        assertNotSame(
            "an index created after the file changed must not join an analyzer built from the old rules",
            one,
            build(IndexVersion.current(), synonymChain("synonyms_path", "one.txt", false))
        );
    }

    /** Nothing reloads an updateable file on change, so a shared instance could serve rules the file no longer has. */
    public void testUpdateableSynonymsFileNeverShares() throws IOException {
        writeConfigFile("one.txt", "a, b");
        Settings chain = synonymChain("synonyms_path", "one.txt", true);
        assertNotSame(build(IndexVersion.current(), chain), build(IndexVersion.current(), chain));
    }
}
