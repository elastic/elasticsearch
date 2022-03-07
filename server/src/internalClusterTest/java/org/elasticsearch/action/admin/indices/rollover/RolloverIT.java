/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLogAppender;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class RolloverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testRolloverOnEmptyIndex() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-1").addAlias(testAlias).get());
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-1");
        if (explicitWriteIndex) {
            assertTrue(oldIndex.getAliases().containsKey("test_alias"));
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
        final IndexMetadata newIndex = state.metadata().index("test_index-000002");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
    }

    public void testRollover() throws Exception {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias")).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getAlias(), equalTo("test_alias"));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get("test_alias").getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    public void testRolloverWithExplicitWriteIndex() throws Exception {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        assertAcked(prepareCreate("test_index-2").addAlias(new Alias("test_alias").writeIndex(true)).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getAlias(), equalTo("test_alias"));
        assertThat(oldIndex.getRolloverInfos().get("test_alias").getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get("test_alias").getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    public void testRolloverWithNoWriteIndex() {
        Boolean firstIsWriteIndex = randomFrom(false, null);
        assertAcked(prepareCreate("index1").addAlias(new Alias("alias").writeIndex(firstIsWriteIndex)).get());
        if (firstIsWriteIndex == null) {
            assertAcked(prepareCreate("index2").addAlias(new Alias("alias").writeIndex(randomFrom(false, null))).get());
        }
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("alias").dryRun(randomBoolean()).get()
        );
        assertThat(exception.getMessage(), equalTo("rollover target [alias] does not point to a write index"));
    }

    public void testRolloverWithIndexSettings() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(settings)
            .alias(new Alias("extra_alias"))
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(1));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(0));
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().containsKey("extra_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverWithIndexSettingsWithoutPrefix() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-2").addAlias(testAlias).get());
        indexDoc("test_index-2", "1", "field", "value");
        flush("test_index-2");
        final Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .settings(settings)
            .alias(new Alias("extra_alias"))
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index-2"));
        assertThat(response.getNewIndex(), equalTo("test_index-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-2");
        final IndexMetadata newIndex = state.metadata().index("test_index-000003");
        assertThat(newIndex.getNumberOfShards(), equalTo(1));
        assertThat(newIndex.getNumberOfReplicas(), equalTo(0));
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        assertTrue(newIndex.getAliases().containsKey("extra_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverDryRun() throws Exception {
        if (randomBoolean()) {
            PutIndexTemplateRequestBuilder putTemplate = client().admin()
                .indices()
                .preparePutTemplate("test_index")
                .setPatterns(List.of("test_index-*"))
                .setOrder(-1)
                .setSettings(Settings.builder().put(AutoExpandReplicas.SETTING.getKey(), "0-all"));
            assertAcked(putTemplate.get());
        }
        assertAcked(prepareCreate("test_index-1").addAlias(new Alias("test_alias")).get());
        indexDoc("test_index-1", "1", "field", "value");
        flush("test_index-1");
        ensureGreen();
        Logger allocationServiceLogger = LogManager.getLogger(AllocationService.class);

        MockLogAppender appender = new MockLogAppender();
        appender.start();
        appender.addExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "no related message logged on dry run",
                AllocationService.class.getName(),
                Level.INFO,
                "*test_index*"
            )
        );
        Loggers.addAppender(allocationServiceLogger, appender);

        final RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").dryRun(true).get();

        appender.assertAllExpectationsMatched();
        appender.stop();
        Loggers.removeAppender(allocationServiceLogger, appender);

        assertThat(response.getOldIndex(), equalTo("test_index-1"));
        assertThat(response.getNewIndex(), equalTo("test_index-000002"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-1");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        final IndexMetadata newIndex = state.metadata().index("test_index-000002");
        assertNull(newIndex);
    }

    public void testRolloverConditionsNotMet() throws Exception {
        boolean explicitWriteIndex = randomBoolean();
        Alias testAlias = new Alias("test_alias");
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index-0").addAlias(testAlias).get());
        indexDoc("test_index-0", "1", "field", "value");
        flush("test_index-0");
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .addMaxIndexSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB))
            .addMaxIndexAgeCondition(TimeValue.timeValueHours(4))
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index-0"));
        assertThat(response.getNewIndex(), equalTo("test_index-000001"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(2));
        assertThat(response.getConditionStatus().values(), everyItem(is(false)));
        Set<String> conditions = response.getConditionStatus().keySet();
        assertThat(
            conditions,
            containsInAnyOrder(
                new MaxSizeCondition(new ByteSizeValue(10, ByteSizeUnit.MB)).toString(),
                new MaxAgeCondition(TimeValue.timeValueHours(4)).toString()
            )
        );

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index-0");
        assertTrue(oldIndex.getAliases().containsKey("test_alias"));
        if (explicitWriteIndex) {
            assertTrue(oldIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertNull(oldIndex.getAliases().get("test_alias").writeIndex());
        }
        final IndexMetadata newIndex = state.metadata().index("test_index-000001");
        assertNull(newIndex);
    }

    public void testRolloverWithNewIndexName() throws Exception {
        Alias testAlias = new Alias("test_alias");
        boolean explicitWriteIndex = randomBoolean();
        if (explicitWriteIndex) {
            testAlias.writeIndex(true);
        }
        assertAcked(prepareCreate("test_index").addAlias(testAlias).get());
        indexDoc("test_index", "1", "field", "value");
        flush("test_index");
        final RolloverResponse response = client().admin()
            .indices()
            .prepareRolloverIndex("test_alias")
            .setNewIndexName("test_new_index")
            .get();
        assertThat(response.getOldIndex(), equalTo("test_index"));
        assertThat(response.getNewIndex(), equalTo("test_new_index"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index("test_index");
        final IndexMetadata newIndex = state.metadata().index("test_new_index");
        assertTrue(newIndex.getAliases().containsKey("test_alias"));
        if (explicitWriteIndex) {
            assertFalse(oldIndex.getAliases().get("test_alias").writeIndex());
            assertTrue(newIndex.getAliases().get("test_alias").writeIndex());
        } else {
            assertFalse(oldIndex.getAliases().containsKey("test_alias"));
        }
    }

    public void testRolloverOnExistingIndex() throws Exception {
        assertAcked(prepareCreate("test_index-0").addAlias(new Alias("test_alias")).get());
        indexDoc("test_index-0", "1", "field", "value");
        assertAcked(prepareCreate("test_index-000001").get());
        indexDoc("test_index-000001", "1", "field", "value");
        flush("test_index-0", "test_index-000001");
        try {
            client().admin().indices().prepareRolloverIndex("test_alias").get();
            fail("expected failure due to existing rollover index");
        } catch (ResourceAlreadyExistsException e) {
            assertThat(e.getIndex().getName(), equalTo("test_index-000001"));
        }
    }

    public void testRolloverWithDateMath() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        assumeTrue("only works on the same day", now.plusMinutes(5).getDayOfYear() == now.getDayOfYear());
        String index = "test-" + DateFormatter.forPattern("yyyy.MM.dd").format(now) + "-1";
        String dateMathExp = "<test-{now/d}-1>";
        assertAcked(prepareCreate(dateMathExp).addAlias(new Alias("test_alias")).get());
        ensureGreen(index);
        // now we modify the provided name such that we can test that the pattern is carried on
        client().admin().indices().prepareClose(index).get();
        client().admin()
            .indices()
            .prepareUpdateSettings(index)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, "<test-{now/M{yyyy.MM}}-1>"))
            .get();

        client().admin().indices().prepareOpen(index).get();
        ensureGreen(index);
        RolloverResponse response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo(index));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000002"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        response = client().admin().indices().prepareRolloverIndex("test_alias").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000002"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000003"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));

        GetSettingsResponse getSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(response.getOldIndex(), response.getNewIndex())
            .get();
        assertEquals(
            "<test-{now/M{yyyy.MM}}-000002>",
            getSettingsResponse.getSetting(response.getOldIndex(), IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );
        assertEquals(
            "<test-{now/M{yyyy.MM}}-000003>",
            getSettingsResponse.getSetting(response.getNewIndex(), IndexMetadata.SETTING_INDEX_PROVIDED_NAME)
        );

        response = client().admin().indices().prepareRolloverIndex("test_alias").setNewIndexName("<test-{now/d}-000004>").get();
        assertThat(response.getOldIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM").format(now) + "-000003"));
        assertThat(response.getNewIndex(), equalTo("test-" + DateFormatter.forPattern("yyyy.MM.dd").format(now) + "-000004"));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
    }

    public void testRolloverMaxSize() throws Exception {
        assertAcked(prepareCreate("test-1").addAlias(new Alias("test_alias")).get());
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc("test-1", Integer.toString(i), "field", "foo-" + i);
        }
        flush("test-1");
        refresh("test_alias");

        // A large max_size
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomIntBetween(100, 50 * 1024), ByteSizeUnit.MB))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("No rollover with a large max_size condition", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }

        // A small max_size
        {
            ByteSizeValue maxSizeValue = new ByteSizeValue(randomIntBetween(1, 20), ByteSizeUnit.BYTES);
            long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(maxSizeValue)
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("Should rollover with a small max_size condition", response.isRolledOver(), equalTo(true));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            List<Condition<?>> metConditions = oldIndex.getRolloverInfos().get("test_alias").getMetConditions();
            assertThat(metConditions.size(), equalTo(1));
            assertThat(metConditions.get(0).toString(), equalTo(new MaxSizeCondition(maxSizeValue).toString()));
            assertThat(
                oldIndex.getRolloverInfos().get("test_alias").getTime(),
                is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
            );
        }

        // An empty index
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxIndexSizeCondition(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-000002"));
            assertThat(response.getNewIndex(), equalTo("test-000003"));
            assertThat("No rollover with an empty index", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-000002");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }
    }

    public void testRolloverMaxPrimaryShardSize() throws Exception {
        assertAcked(prepareCreate("test-1").addAlias(new Alias("test_alias")).get());
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc("test-1", Integer.toString(i), "field", "foo-" + i);
        }
        flush("test-1");
        refresh("test_alias");

        // A large max_primary_shard_size
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxPrimaryShardSizeCondition(new ByteSizeValue(randomIntBetween(100, 50 * 1024), ByteSizeUnit.MB))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("No rollover with a large max_primary_shard_size condition", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }

        // A small max_primary_shard_size
        {
            ByteSizeValue maxPrimaryShardSizeCondition = new ByteSizeValue(randomIntBetween(1, 20), ByteSizeUnit.BYTES);
            long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxPrimaryShardSizeCondition(maxPrimaryShardSizeCondition)
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("Should rollover with a small max_primary_shard_size condition", response.isRolledOver(), equalTo(true));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            List<Condition<?>> metConditions = oldIndex.getRolloverInfos().get("test_alias").getMetConditions();
            assertThat(metConditions.size(), equalTo(1));
            assertThat(metConditions.get(0).toString(), equalTo(new MaxPrimaryShardSizeCondition(maxPrimaryShardSizeCondition).toString()));
            assertThat(
                oldIndex.getRolloverInfos().get("test_alias").getTime(),
                is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
            );
        }

        // An empty index
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxPrimaryShardSizeCondition(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-000002"));
            assertThat(response.getNewIndex(), equalTo("test-000003"));
            assertThat("No rollover with an empty index", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-000002");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }
    }

    public void testRolloverMaxPrimaryShardDocs() throws Exception {
        assertAcked(
            prepareCreate("test-1").setSettings(Settings.builder().put("index.number_of_shards", 1)).addAlias(new Alias("test_alias")).get()
        );
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc("test-1", Integer.toString(i), "field", "foo-" + i);
        }
        flush("test-1");
        refresh("test_alias");

        // A large max_primary_shard_docs
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxPrimaryShardDocsCondition(randomIntBetween(21, 30))
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("No rollover with a large max_primary_shard_docs condition", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }

        // A small max_primary_shard_docs
        {
            MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(randomLongBetween(1, 9));
            long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocsCondition.value)
                .get();
            assertThat(response.getOldIndex(), equalTo("test-1"));
            assertThat(response.getNewIndex(), equalTo("test-000002"));
            assertThat("Should rollover with a small max_primary_shard_docs condition", response.isRolledOver(), equalTo(true));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-1");
            List<Condition<?>> metConditions = oldIndex.getRolloverInfos().get("test_alias").getMetConditions();
            assertThat(metConditions.size(), equalTo(1));
            assertThat(
                metConditions.get(0).toString(),
                equalTo(new MaxPrimaryShardDocsCondition(maxPrimaryShardDocsCondition.value).toString())
            );
            assertThat(
                oldIndex.getRolloverInfos().get("test_alias").getTime(),
                is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
            );
        }

        // An empty index
        {
            final RolloverResponse response = client().admin()
                .indices()
                .prepareRolloverIndex("test_alias")
                .addMaxPrimaryShardDocsCondition(randomNonNegativeLong())
                .get();
            assertThat(response.getOldIndex(), equalTo("test-000002"));
            assertThat(response.getNewIndex(), equalTo("test-000003"));
            assertThat("No rollover with an empty index", response.isRolledOver(), equalTo(false));
            final IndexMetadata oldIndex = client().admin().cluster().prepareState().get().getState().metadata().index("test-000002");
            assertThat(oldIndex.getRolloverInfos().size(), equalTo(0));
        }
    }

    public void testRejectIfAliasFoundInTemplate() throws Exception {
        client().admin()
            .indices()
            .preparePutTemplate("logs")
            .setPatterns(Collections.singletonList("logs-*"))
            .addAlias(new Alias("logs-write"))
            .get();
        assertAcked(client().admin().indices().prepareCreate("logs-000001").get());
        ensureYellow("logs-write");
        final IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareRolloverIndex("logs-write").get()
        );
        assertThat(
            error.getMessage(),
            equalTo(
                "Rollover alias [logs-write] can point to multiple indices, found duplicated alias [[logs-write]] in index template [logs]"
            )
        );
    }

    public void testRolloverWithClosedIndexInAlias() {
        final String aliasName = "alias";
        final String openNonwriteIndex = "open-index-nonwrite";
        final String closedIndex = "closed-index-nonwrite";
        final String writeIndexPrefix = "write-index-";
        assertAcked(prepareCreate(openNonwriteIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(closedIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());
        ensureGreen();

        index(closedIndex, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        refresh(aliasName);

        assertAcked(client().admin().indices().prepareClose(closedIndex).setTimeout(TimeValue.timeValueSeconds(60)).get());

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(aliasName).addMaxIndexDocsCondition(1).get();
        assertTrue(rolloverResponse.isRolledOver());
        assertEquals(writeIndexPrefix + "000001", rolloverResponse.getOldIndex());
        assertEquals(writeIndexPrefix + "000002", rolloverResponse.getNewIndex());
    }

    public void testRolloverWithClosedWriteIndex() throws Exception {
        final String aliasName = "alias";
        final String openNonwriteIndex = "open-index-nonwrite";
        final String closedIndex = "closed-index-nonwrite";
        final String writeIndexPrefix = "write-index-";
        assertAcked(prepareCreate(openNonwriteIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(closedIndex).addAlias(new Alias(aliasName)).get());
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());

        index(closedIndex, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        index(aliasName, null, "{\"foo\": \"bar\"}");
        refresh(aliasName);

        assertAcked(client().admin().indices().prepareClose(closedIndex).get());
        assertAcked(client().admin().indices().prepareClose(writeIndexPrefix + "000001").get());
        ensureGreen(aliasName);

        RolloverResponse rolloverResponse = client().admin().indices().prepareRolloverIndex(aliasName).addMaxIndexDocsCondition(1).get();
        assertTrue(rolloverResponse.isRolledOver());
        assertEquals(writeIndexPrefix + "000001", rolloverResponse.getOldIndex());
        assertEquals(writeIndexPrefix + "000002", rolloverResponse.getNewIndex());
    }

    public void testRolloverWithHiddenAliasesAndExplicitWriteIndex() {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        final String indexNamePrefix = "test_index_hidden-";
        final String firstIndexName = indexNamePrefix + "000001";
        final String secondIndexName = indexNamePrefix + "000002";

        final String aliasName = "test_alias";
        assertAcked(prepareCreate(firstIndexName).addAlias(new Alias(aliasName).writeIndex(true).isHidden(true)).get());
        indexDoc(aliasName, "1", "field", "value");
        refresh();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();
        assertThat(response.getOldIndex(), equalTo(firstIndexName));
        assertThat(response.getNewIndex(), equalTo(secondIndexName));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index(firstIndexName);
        assertTrue(oldIndex.getAliases().containsKey(aliasName));
        assertTrue(oldIndex.getAliases().get(aliasName).isHidden());
        assertFalse(oldIndex.getAliases().get(aliasName).writeIndex());
        final IndexMetadata newIndex = state.metadata().index(secondIndexName);
        assertTrue(newIndex.getAliases().containsKey(aliasName));
        assertTrue(newIndex.getAliases().get(aliasName).isHidden());
        assertTrue(newIndex.getAliases().get(aliasName).writeIndex());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getAlias(), equalTo(aliasName));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get(aliasName).getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    public void testRolloverWithHiddenAliasesAndImplicitWriteIndex() {
        long beforeTime = client().threadPool().absoluteTimeInMillis() - 1000L;
        final String indexNamePrefix = "test_index_hidden-";
        final String firstIndexName = indexNamePrefix + "000001";
        final String secondIndexName = indexNamePrefix + "000002";

        final String aliasName = "test_alias";
        assertAcked(prepareCreate(firstIndexName).addAlias(new Alias(aliasName).isHidden(true)).get());
        indexDoc(aliasName, "1", "field", "value");
        refresh();
        final RolloverResponse response = client().admin().indices().prepareRolloverIndex(aliasName).get();
        assertThat(response.getOldIndex(), equalTo(firstIndexName));
        assertThat(response.getNewIndex(), equalTo(secondIndexName));
        assertThat(response.isDryRun(), equalTo(false));
        assertThat(response.isRolledOver(), equalTo(true));
        assertThat(response.getConditionStatus().size(), equalTo(0));
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final IndexMetadata oldIndex = state.metadata().index(firstIndexName);
        assertFalse(oldIndex.getAliases().containsKey(aliasName));
        final IndexMetadata newIndex = state.metadata().index(secondIndexName);
        assertTrue(newIndex.getAliases().containsKey(aliasName));
        assertTrue(newIndex.getAliases().get(aliasName).isHidden());
        assertThat(newIndex.getAliases().get(aliasName).writeIndex(), nullValue());
        assertThat(oldIndex.getRolloverInfos().size(), equalTo(1));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getAlias(), equalTo(aliasName));
        assertThat(oldIndex.getRolloverInfos().get(aliasName).getMetConditions(), is(empty()));
        assertThat(
            oldIndex.getRolloverInfos().get(aliasName).getTime(),
            is(both(greaterThanOrEqualTo(beforeTime)).and(lessThanOrEqualTo(client().threadPool().absoluteTimeInMillis() + 1000L)))
        );
    }

    /**
     * Tests that multiple threads all racing to rollover based on a condition trigger one and only one rollover
     */
    public void testMultiThreadedRollover() throws Exception {
        final String aliasName = "alias";
        final String writeIndexPrefix = "tt-";
        assertAcked(prepareCreate(writeIndexPrefix + "000001").addAlias(new Alias(aliasName).writeIndex(true)).get());
        ensureGreen();

        final int threadCount = randomIntBetween(5, 10);
        final CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        Set<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(() -> {
            try {
                logger.info("--> [{}] waiting for all the other threads before starting", i);
                barrier.await();
                while (running.get()) {
                    RolloverResponse resp = client().admin().indices().prepareRolloverIndex(aliasName).addMaxIndexDocsCondition(1).get();
                    if (resp.isRolledOver()) {
                        logger.info("--> thread [{}] successfully rolled over: {}", i, Strings.toString(resp));
                        assertThat(resp.getOldIndex(), equalTo(writeIndexPrefix + "000001"));
                        assertThat(resp.getNewIndex(), equalTo(writeIndexPrefix + "000002"));
                    }
                }
            } catch (Exception e) {
                logger.error(new ParameterizedMessage("thread [{}] encountered unexpected exception", i), e);
                fail("we should not encounter unexpected exceptions");
            }
        }, "rollover-thread-" + i)).collect(Collectors.toSet());

        threads.forEach(Thread::start);

        // Okay, signal the floodgates to open
        barrier.await();

        index(aliasName, null, "{\"foo\": \"bar\"}");

        assertBusy(() -> {
            try {
                client().admin().indices().prepareGetIndex().addIndices(writeIndexPrefix + "000002").get();
            } catch (Exception e) {
                logger.info("--> expecting second index to be created but it has not yet been created");
                fail("expecting second index to exist");
            }
        });

        // Tell everyone to stop trying to roll over
        running.set(false);

        threads.forEach(thread -> {
            try {
                thread.join(1000);
            } catch (Exception e) {
                logger.warn("expected thread to be stopped, but got", e);
            }
        });

        // We should *NOT* have a third index, it should have rolled over *exactly* once
        expectThrows(Exception.class, () -> client().admin().indices().prepareGetIndex().addIndices(writeIndexPrefix + "000003").get());
    }

    public void testRolloverConcurrently() throws Exception {
        int numOfThreads = 5;
        int numberOfRolloversPerThread = 20;

        var putTemplateRequest = new PutComposableIndexTemplateAction.Request("my-template");
        var template = new Template(
            Settings.builder()
                // Avoid index check, which gets randomly inserted by test framework. This slows down the test a bit.
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build(),
            null,
            null
        );
        putTemplateRequest.indexTemplate(new ComposableIndexTemplate(List.of("test-*"), template, null, 100L, null, null));
        assertAcked(client().execute(PutComposableIndexTemplateAction.INSTANCE, putTemplateRequest).actionGet());

        final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
        final Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < numOfThreads; i++) {
            var aliasName = "test-" + i;
            threads[i] = new Thread(() -> {
                assertAcked(prepareCreate(aliasName + "-000001").addAlias(new Alias(aliasName).writeIndex(true)).get());
                for (int j = 1; j <= numberOfRolloversPerThread; j++) {
                    try {
                        barrier.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    var response = client().admin()
                        .indices()
                        .prepareRolloverIndex(aliasName)
                        .waitForActiveShards(ActiveShardCount.NONE)
                        .get();
                    assertThat(response.getOldIndex(), equalTo(aliasName + String.format(Locale.ROOT, "-%06d", j)));
                    assertThat(response.getNewIndex(), equalTo(aliasName + String.format(Locale.ROOT, "-%06d", j + 1)));
                    assertThat(response.isDryRun(), equalTo(false));
                    assertThat(response.isRolledOver(), equalTo(true));
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (int i = 0; i < numOfThreads; i++) {
            var aliasName = "test-" + i;
            var response = client().admin().indices().getAliases(new GetAliasesRequest(aliasName)).get();
            List<Map.Entry<String, List<AliasMetadata>>> actual = response.getAliases().entrySet().stream().toList();
            List<Map.Entry<String, List<AliasMetadata>>> expected = new ArrayList<>(numberOfRolloversPerThread);
            int numOfIndices = numberOfRolloversPerThread + 1;
            for (int j = 1; j <= numOfIndices; j++) {
                AliasMetadata.Builder amBuilder = new AliasMetadata.Builder(aliasName);
                amBuilder.writeIndex(j == numOfIndices);
                expected.add(Map.entry(aliasName + String.format(Locale.ROOT, "-%06d", j), List.of(amBuilder.build())));
            }
            assertThat(actual, containsInAnyOrder(expected.toArray(Object[]::new)));
        }
    }

}
