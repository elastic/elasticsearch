/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataRolloverServiceTests extends ESTestCase {

    public void testRolloverAliasActions() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);

        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, false, null, sourceAlias);
        assertThat(actions, hasSize(2));
        boolean foundAdd = false;
        boolean foundRemove = false;
        for (AliasAction action : actions) {
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, ((AliasAction.Add) action).getAlias());
                foundAdd = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, ((AliasAction.Remove) action).getAlias());
                foundRemove = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAdd);
        assertTrue(foundRemove);
    }

    public void testRolloverAliasActionsWithExplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, true, null, sourceAlias);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            AliasAction.Add addAction = (AliasAction.Add) action;
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertTrue(addAction.writeIndex());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertFalse(addAction.writeIndex());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testRolloverAliasActionsWithHiddenAliasAndExplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, true, true, sourceAlias);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            assertThat(action, instanceOf(AliasAction.Add.class));
            AliasAction.Add addAction = (AliasAction.Add) action;
            if (action.getIndex().equals(targetIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertTrue(addAction.writeIndex());
                assertTrue(addAction.isHidden());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertEquals(sourceAlias, addAction.getAlias());
                assertFalse(addAction.writeIndex());
                assertTrue(addAction.isHidden());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testRolloverAliasActionsWithHiddenAliasAndImplicitWriteIndex() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);
        List<AliasAction> actions = MetaDataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, false, true, sourceAlias);

        assertThat(actions, hasSize(2));
        boolean foundAddWrite = false;
        boolean foundRemoveWrite = false;
        for (AliasAction action : actions) {
            if (action.getIndex().equals(targetIndex)) {
                assertThat(action, instanceOf(AliasAction.Add.class));
                AliasAction.Add addAction = (AliasAction.Add) action;
                assertEquals(sourceAlias, addAction.getAlias());
                assertThat(addAction.writeIndex(), nullValue());
                assertTrue(addAction.isHidden());
                foundAddWrite = true;
            } else if (action.getIndex().equals(sourceIndex)) {
                assertThat(action, instanceOf(AliasAction.Remove.class));
                AliasAction.Remove removeAction = (AliasAction.Remove) action;
                assertEquals(sourceAlias, removeAction.getAlias());
                foundRemoveWrite = true;
            } else {
                throw new AssertionError("Unknown index [" + action.getIndex() + "]");
            }
        }
        assertTrue(foundAddWrite);
        assertTrue(foundRemoveWrite);
    }

    public void testValidation() {
        String index1 = randomAlphaOfLength(10);
        String aliasWithWriteIndex = randomAlphaOfLength(10);
        String index2 = randomAlphaOfLength(10);
        String aliasWithNoWriteIndex = randomAlphaOfLength(10);
        Boolean firstIsWriteIndex = randomFrom(false, null);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        MetaData.Builder metaDataBuilder = MetaData.builder()
            .put(IndexMetaData.builder(index1)
                .settings(settings)
                .putAlias(AliasMetaData.builder(aliasWithWriteIndex))
                .putAlias(AliasMetaData.builder(aliasWithNoWriteIndex).writeIndex(firstIsWriteIndex))
            );
        IndexMetaData.Builder indexTwoBuilder = IndexMetaData.builder(index2).settings(settings);
        if (firstIsWriteIndex == null) {
            indexTwoBuilder.putAlias(AliasMetaData.builder(aliasWithNoWriteIndex).writeIndex(randomFrom(false, null)));
        }
        metaDataBuilder.put(indexTwoBuilder);
        MetaData metaData = metaDataBuilder.build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.validate(metaData, aliasWithNoWriteIndex));
        assertThat(exception.getMessage(), equalTo("source alias [" + aliasWithNoWriteIndex + "] does not point to a write index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.validate(metaData, randomFrom(index1, index2)));
        assertThat(exception.getMessage(), equalTo("source alias is a concrete index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.validate(metaData, randomAlphaOfLength(5))
        );
        assertThat(exception.getMessage(), equalTo("source alias does not exist"));
        MetaDataRolloverService.validate(metaData, aliasWithWriteIndex);
    }

    public void testGenerateRolloverIndexName() {
        String invalidIndexName = randomAlphaOfLength(10) + "A";
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.generateRolloverIndexName(invalidIndexName, indexNameExpressionResolver));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAlphaOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(MetaDataRolloverService.generateRolloverIndexName(indexEndingInNumbers, indexNameExpressionResolver),
            equalTo(indexPrefix + "-" + String.format(Locale.ROOT, "%06d", num + 1)));
        assertThat(MetaDataRolloverService.generateRolloverIndexName("index-name-1", indexNameExpressionResolver),
            equalTo("index-name-000002"));
        assertThat(MetaDataRolloverService.generateRolloverIndexName("index-name-2", indexNameExpressionResolver),
            equalTo("index-name-000003"));
        assertEquals( "<index-name-{now/d}-000002>", MetaDataRolloverService.generateRolloverIndexName("<index-name-{now/d}-1>",
            indexNameExpressionResolver));
    }

    public void testCreateIndexRequest() {
        String alias = randomAlphaOfLength(10);
        String rolloverIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest =
            MetaDataRolloverService.prepareCreateIndexRequest(rolloverIndex, rolloverIndex, rolloverRequest.getCreateIndexRequest());
        assertThat(createIndexRequest.settings(), equalTo(settings));
        assertThat(createIndexRequest.index(), equalTo(rolloverIndex));
        assertThat(createIndexRequest.cause(), equalTo("rollover_index"));
    }

    public void testRejectDuplicateAlias() {
        final IndexTemplateMetaData template = IndexTemplateMetaData.builder("test-template")
            .patterns(Arrays.asList("foo-*", "bar-*"))
            .putAlias(AliasMetaData.builder("foo-write")).putAlias(AliasMetaData.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final MetaData metaData = MetaData.builder().put(createMetaData(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> MetaDataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName, randomBoolean()));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedTemplates() {
        final IndexTemplateMetaData template = IndexTemplateMetaData.builder("test-template")
            .patterns(Collections.singletonList("*"))
            .putAlias(AliasMetaData.builder("foo-write")).putAlias(AliasMetaData.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final MetaData metaData = MetaData.builder().put(createMetaData(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetaDataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            MetaDataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metaData, indexName, aliasName, randomFrom(Boolean.FALSE, null)));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    /**
     * Test the main rolloverClusterState method. This does not validate every detail to depth, rather focuses on observing that each
     * parameter is used for the purpose intended.
     */
    public void testRolloverClusterState() throws Exception {
        final String aliasName = "logs-alias";
        final String indexPrefix = "logs-index-00000";
        String sourceIndexName = indexPrefix + "1";
        final IndexMetaData.Builder indexMetaData = IndexMetaData.builder(sourceIndexName)
            .putAlias(AliasMetaData.builder(aliasName).writeIndex(true).build()).settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(1);
        final ClusterState clusterState =
            ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder().put(indexMetaData)).build();

        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool);
            Environment env = mock(Environment.class);
            when(env.sharedDataFile()).thenReturn(null);
            AllocationService allocationService = mock(AllocationService.class);
            when(allocationService.reroute(any(ClusterState.class), any(String.class))).then(i -> i.getArguments()[0]);
            IndicesService indicesService = mockIndicesServices();
            IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
            when(mockIndexNameExpressionResolver.resolveDateMathExpression(any())).then(returnsFirstArg());

            MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(Settings.EMPTY,
                clusterService, indicesService, allocationService, null, env, null, testThreadPool, null, Collections.emptyList(), false);
            MetaDataIndexAliasesService indexAliasesService = new MetaDataIndexAliasesService(clusterService, indicesService,
                new AliasValidator(), null, xContentRegistry());
            MetaDataRolloverService rolloverService = new MetaDataRolloverService(testThreadPool, createIndexService, indexAliasesService,
                mockIndexNameExpressionResolver);

            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            String newIndexName = randomBoolean() ? "logs-index-9" : null;
            int numberOfShards = randomIntBetween(1, 5);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
            createIndexRequest.settings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards));

            long before = testThreadPool.absoluteTimeInMillis();
            MetaDataRolloverService.RolloverResult rolloverResult =
                rolloverService.rolloverClusterState(clusterState,aliasName, newIndexName, createIndexRequest, metConditions,
                    randomBoolean());
            long after = testThreadPool.absoluteTimeInMillis();

            newIndexName = newIndexName == null ? indexPrefix + "2" : newIndexName;
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName);
            assertEquals(newIndexName, rolloverResult.rolloverIndexName);
            MetaData rolloverMetaData = rolloverResult.clusterState.metaData();
            assertEquals(2, rolloverMetaData.indices().size());
            IndexMetaData rolloverIndexMetaData = rolloverMetaData.index(newIndexName);
            assertThat(rolloverIndexMetaData.getNumberOfShards(), equalTo(numberOfShards));

            AliasOrIndex.Alias alias = (AliasOrIndex.Alias) rolloverMetaData.getAliasAndIndexLookup().get(aliasName);
            assertThat(alias.getIndices(), hasSize(2));
            assertThat(alias.getIndices(), hasItem(rolloverMetaData.index(sourceIndexName)));
            assertThat(alias.getIndices(), hasItem(rolloverIndexMetaData));
            assertThat(alias.getWriteIndex(), equalTo(rolloverIndexMetaData));

            RolloverInfo info = rolloverMetaData.index(sourceIndexName).getRolloverInfos().get(aliasName);
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));
        } finally {
            testThreadPool.shutdown();
        }
    }

    private IndicesService mockIndicesServices() throws java.io.IOException {
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.withTempIndexService(any(IndexMetaData.class), any(CheckedFunction.class)))
            .then(invocationOnMock -> {
                IndexService indexService = mock(IndexService.class);
                IndexMetaData indexMetaData = (IndexMetaData) invocationOnMock.getArguments()[0];
                when(indexService.index()).thenReturn(indexMetaData.getIndex());
                MapperService mapperService = mock(MapperService.class);
                when(indexService.mapperService()).thenReturn(mapperService);
                when(mapperService.documentMapper()).thenReturn(null);
                when(indexService.getIndexEventListener()).thenReturn(new IndexEventListener() {});
                when(indexService.getIndexSortSupplier()).thenReturn(() -> null);
                //noinspection unchecked
                return ((CheckedFunction) invocationOnMock.getArguments()[1]).apply(indexService);
            });
        return indicesService;
    }

    private static IndexMetaData createMetaData(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetaData.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }
}
