/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class AutoCreateIndexTests extends ESTestCase {

    private static final String TEST_SYSTEM_INDEX_NAME = ".test-system-index";

    public void testParseFailed() {
        try {
            Settings settings = Settings.builder().put("action.auto_create_index", ",,,").build();
            newAutoCreateIndex(settings);
            fail("initialization should have failed");
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "Can't parse [,,,] for setting [action.auto_create_index] must be either [true, false, or a "
                    + "comma separated list of index patterns]",
                ex.getMessage()
            );
        }
    }

    public void testParseFailedMissingIndex() {
        String prefix = randomFrom("+", "-");
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), prefix).build();
        try {
            newAutoCreateIndex(settings);
            fail("initialization should have failed");
        } catch (IllegalArgumentException ex) {
            assertEquals(
                "Can't parse [" + prefix + "] for setting [action.auto_create_index] must contain an index name after [" + prefix + "]",
                ex.getMessage()
            );
        }
    }

    public void testHandleSpaces() { // see #21449
        Settings settings = Settings.builder()
            .put(
                AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(),
                randomFrom(
                    ".marvel-, .security, .watches, .triggered_watches, .watcher-history-",
                    ".marvel-,.security,.watches,.triggered_watches,.watcher-history-"
                )
            )
            .build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        List<Tuple<String, Boolean>> expressions = autoCreateIndex.getAutoCreate().getExpressions();
        Map<String, Boolean> map = new HashMap<>();
        for (Tuple<String, Boolean> t : expressions) {
            map.put(t.v1(), t.v2());
        }
        assertTrue(map.get(".marvel-"));
        assertTrue(map.get(".security"));
        assertTrue(map.get(".watches"));
        assertTrue(map.get(".triggered_watches"));
        assertTrue(map.get(".watcher-history-"));
        assertEquals(5, map.size());
    }

    public void testAutoCreationDisabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        String randomIndex = randomAlphaOfLengthBetween(1, 10);
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> autoCreateIndex.shouldAutoCreate(randomIndex, buildClusterState())
        );
        assertEquals("no such index [" + randomIndex + "] and [action.auto_create_index] is [false]", e.getMessage());
    }

    public void testSystemIndexWithAutoCreationDisabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        assertThat(autoCreateIndex.shouldAutoCreate(TEST_SYSTEM_INDEX_NAME, buildClusterState()), equalTo(true));
    }

    public void testAutoCreationEnabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true).build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        assertThat(autoCreateIndex.shouldAutoCreate(randomAlphaOfLengthBetween(1, 10), buildClusterState()), equalTo(true));
    }

    public void testDefaultAutoCreation() {
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(Settings.EMPTY);
        assertThat(autoCreateIndex.shouldAutoCreate(randomAlphaOfLengthBetween(1, 10), buildClusterState()), equalTo(true));
    }

    public void testExistingIndex() {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom(true, false, randomAlphaOfLengthBetween(7, 10)).toString())
            .build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        assertThat(
            autoCreateIndex.shouldAutoCreate(randomFrom("index1", "index2", "index3"), buildClusterState("index1", "index2", "index3")),
            equalTo(false)
        );
    }

    public void testAutoCreationPatternEnabled() {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom("+index*", "index*"))
            .build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("index" + randomAlphaOfLengthBetween(1, 5), clusterState), equalTo(true));
        expectNotMatch(clusterState, autoCreateIndex, "does_not_match" + randomAlphaOfLengthBetween(1, 5));
    }

    public void testAutoCreationPatternDisabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "-index*").build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder()).build();
        expectForbidden(clusterState, autoCreateIndex, "index" + randomAlphaOfLengthBetween(1, 5), "-index*");
        /* When patterns are specified, even if the are all negative, the default is can't create. So a pure negative pattern is the same
         * as false, really. */
        expectNotMatch(clusterState, autoCreateIndex, "does_not_match" + randomAlphaOfLengthBetween(1, 5));
    }

    public void testAutoCreationSystemIndexPatternDisabled() {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "-" + TEST_SYSTEM_INDEX_NAME + "*")
            .build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        assertThat(autoCreateIndex.shouldAutoCreate(TEST_SYSTEM_INDEX_NAME, buildClusterState()), equalTo(true));
    }

    public void testAutoCreationMultiplePatternsWithWildcards() {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom("+test*,-index*", "test*,-index*"))
            .build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder()).build();
        expectForbidden(clusterState, autoCreateIndex, "index" + randomAlphaOfLengthBetween(1, 5), "-index*");
        assertThat(autoCreateIndex.shouldAutoCreate("test" + randomAlphaOfLengthBetween(1, 5), clusterState), equalTo(true));
        expectNotMatch(clusterState, autoCreateIndex, "does_not_match" + randomAlphaOfLengthBetween(1, 5));
    }

    public void testAutoCreationMultiplePatternsNoWildcards() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "+test1,-index1").build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("test1", clusterState), equalTo(true));
        expectNotMatch(clusterState, autoCreateIndex, "index" + randomAlphaOfLengthBetween(1, 5));
        expectNotMatch(clusterState, autoCreateIndex, "test" + randomAlphaOfLengthBetween(2, 5));
        expectNotMatch(clusterState, autoCreateIndex, "does_not_match" + randomAlphaOfLengthBetween(1, 5));
    }

    public void testAutoCreationMultipleIndexNames() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "test1,test2").build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("test1", clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("test2", clusterState), equalTo(true));
        expectNotMatch(clusterState, autoCreateIndex, "does_not_match" + randomAlphaOfLengthBetween(1, 5));
    }

    public void testAutoCreationConflictingPatternsFirstWins() {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "+test1,-test1,-test2,+test2")
            .build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("test1", clusterState), equalTo(true));
        expectForbidden(clusterState, autoCreateIndex, "test2", "-test2");
        expectNotMatch(clusterState, autoCreateIndex, "does_not_match" + randomAlphaOfLengthBetween(1, 5));
    }

    public void testUpdate() {
        boolean value = randomBoolean();
        Settings settings;
        if (value && randomBoolean()) {
            settings = Settings.EMPTY;
        } else {
            settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), value).build();
        }

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SystemIndices systemIndices = EmptySystemIndices.INSTANCE;
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(
            settings,
            clusterSettings,
            TestIndexNameExpressionResolver.newInstance(systemIndices),
            systemIndices
        );
        assertThat(autoCreateIndex.getAutoCreate().isAutoCreateIndex(), equalTo(value));

        Settings newSettings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), value == false).build();
        clusterSettings.applySettings(newSettings);
        assertThat(autoCreateIndex.getAutoCreate().isAutoCreateIndex(), equalTo(value == false));

        newSettings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "logs-*").build();
        clusterSettings.applySettings(newSettings);
        assertThat(autoCreateIndex.getAutoCreate().isAutoCreateIndex(), equalTo(true));
        assertThat(autoCreateIndex.getAutoCreate().getExpressions().size(), equalTo(1));
        assertThat(autoCreateIndex.getAutoCreate().getExpressions().get(0).v1(), equalTo("logs-*"));
    }

    /**
     * Check that if a template matches the index to be created, but that template does not have a value
     * for the allow_auto_create setting at all, and the auto_create_index setting matches the index
     * to be created, then the null in the template does not override the auto_create_index logic and the
     * index can be created.
     */
    public void testNullAllowAutoCreateInTemplateDoesNotOverrideMatchingAutoCreateIndexSetting() {
        String randomIndex = randomAlphaOfLengthBetween(2, 10);
        final ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(List.of(randomIndex.charAt(0) + "*"))
            .componentTemplates(List.of())
            .metadata(Map.of())
            .build();

        final Metadata metadata = Metadata.builder().indexTemplates(Map.of("test_template", template)).build();
        final ClusterState clusterState = ClusterState.builder(buildClusterState()).metadata(metadata).build();

        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomIndex.charAt(0) + "*").build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        assertTrue(autoCreateIndex.shouldAutoCreate(randomIndex, clusterState));
    }

    /**
     * Check that if a template matches the index to be created, but that template does not have a value
     * for the allow_auto_create setting at all, then it does not cause the auto-create logic to trip over
     * on a null value.
     */
    public void testCanHandleNullAutoCreateSettingInTemplate() {
        String randomIndex = randomAlphaOfLengthBetween(2, 10);
        final ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(List.of(randomIndex.charAt(0) + "*"))
            .componentTemplates(List.of())
            .metadata(Map.of())
            .build();

        final Metadata metadata = Metadata.builder().indexTemplates(Map.of("test_template", template)).build();
        final ClusterState clusterState = ClusterState.builder(buildClusterState()).metadata(metadata).build();

        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> autoCreateIndex.shouldAutoCreate(randomIndex, clusterState)
        );
        assertEquals("no such index [" + randomIndex + "] and [action.auto_create_index] is [false]", e.getMessage());
    }

    /**
     * Check that if a template matches the index to be created, but that template has the allow_auto_create
     * setting turned off, then it overrides the global setting.
     */
    public void testDisabledAutoCreateTemplateSettingDoesNotOverride() {
        String randomIndex = randomAlphaOfLengthBetween(2, 10);
        final ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(List.of(randomIndex.charAt(0) + "*"))
            .componentTemplates(List.of())
            .metadata(Map.of())
            .allowAutoCreate(false)
            .build();

        final Metadata metadata = Metadata.builder().indexTemplates(Map.of("test_template", template)).build();
        final ClusterState clusterState = ClusterState.builder(buildClusterState()).metadata(metadata).build();

        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> autoCreateIndex.shouldAutoCreate(randomIndex, clusterState)
        );
        assertEquals("no such index [composable template [" + randomIndex.charAt(0) + "*] forbids index auto creation]", e.getMessage());
    }

    /**
     * Check that if a template matches the index to be created, and that template has the allow_auto_create
     * setting enabled, then it overrides the global setting.
     */
    public void testEnabledAutoCreateTemplateSettingDoesOverride() {
        String randomIndex = randomAlphaOfLengthBetween(2, 10);
        final ComposableIndexTemplate template = new ComposableIndexTemplate.Builder().indexPatterns(List.of(randomIndex.charAt(0) + "*"))
            .componentTemplates(List.of())
            .metadata(Map.of())
            .allowAutoCreate(true)
            .build();

        final Metadata metadata = Metadata.builder().indexTemplates(Map.of("test_template", template)).build();
        final ClusterState clusterState = ClusterState.builder(buildClusterState()).metadata(metadata).build();

        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = newAutoCreateIndex(settings);
        assertThat(autoCreateIndex.shouldAutoCreate(randomIndex, clusterState), equalTo(true));
    }

    private static ClusterState buildClusterState(String... indices) {
        Metadata.Builder metadata = Metadata.builder();
        for (String index : indices) {
            metadata.put(IndexMetadata.builder(index).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1));
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
    }

    private AutoCreateIndex newAutoCreateIndex(Settings settings) {
        SystemIndices systemIndices = new SystemIndices(
            List.of(
                new SystemIndices.Feature(
                    "plugin",
                    "test feature",
                    List.of(SystemIndexDescriptorUtils.createUnmanaged(TEST_SYSTEM_INDEX_NAME + "*", ""))
                )
            )
        );
        return new AutoCreateIndex(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            TestIndexNameExpressionResolver.newInstance(systemIndices),
            systemIndices
        );
    }

    private void expectNotMatch(ClusterState clusterState, AutoCreateIndex autoCreateIndex, String index) {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> autoCreateIndex.shouldAutoCreate(index, clusterState));
        assertEquals(
            "no such index [" + index + "] and [action.auto_create_index] ([" + autoCreateIndex.getAutoCreate() + "]) doesn't match",
            e.getMessage()
        );
    }

    private void expectForbidden(ClusterState clusterState, AutoCreateIndex autoCreateIndex, String index, String forbiddingPattern) {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> autoCreateIndex.shouldAutoCreate(index, clusterState));
        assertEquals(
            "no such index ["
                + index
                + "] and [action.auto_create_index] contains ["
                + forbiddingPattern
                + "] which forbids automatic creation of the index",
            e.getMessage()
        );
    }
}
