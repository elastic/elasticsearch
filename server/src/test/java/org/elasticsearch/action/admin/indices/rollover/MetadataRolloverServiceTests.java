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
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTests;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

public class MetadataRolloverServiceTests extends ESTestCase {

    public void testRolloverAliasActions() {
        String sourceAlias = randomAlphaOfLength(10);
        String sourceIndex = randomAlphaOfLength(10);
        String targetIndex = randomAlphaOfLength(10);

        List<AliasAction> actions = MetadataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, false, null, sourceAlias);
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
        List<AliasAction> actions = MetadataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, true, null, sourceAlias);

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
        List<AliasAction> actions = MetadataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, true, true, sourceAlias);

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
        List<AliasAction> actions = MetadataRolloverService.rolloverAliasToNewIndex(sourceIndex, targetIndex, false, true, sourceAlias);

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

    public void testAliasValidation() {
        String index1 = randomAlphaOfLength(10);
        String aliasWithWriteIndex = randomAlphaOfLength(10);
        String index2 = randomAlphaOfLength(10);
        String aliasWithNoWriteIndex = randomAlphaOfLength(10);
        Boolean firstIsWriteIndex = randomFrom(false, null);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        Metadata.Builder metadataBuilder = Metadata.builder()
            .put(IndexMetadata.builder(index1)
                .settings(settings)
                .putAlias(AliasMetadata.builder(aliasWithWriteIndex))
                .putAlias(AliasMetadata.builder(aliasWithNoWriteIndex).writeIndex(firstIsWriteIndex))
            );
        IndexMetadata.Builder indexTwoBuilder = IndexMetadata.builder(index2).settings(settings);
        if (firstIsWriteIndex == null) {
            indexTwoBuilder.putAlias(AliasMetadata.builder(aliasWithNoWriteIndex).writeIndex(randomFrom(false, null)));
        }
        metadataBuilder.put(indexTwoBuilder);
        Metadata metadata = metadataBuilder.build();
        CreateIndexRequest req = new CreateIndexRequest();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, aliasWithNoWriteIndex, randomAlphaOfLength(5), req));
        assertThat(exception.getMessage(),
            equalTo("rollover target [" + aliasWithNoWriteIndex + "] does not point to a write index"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, randomFrom(index1, index2), randomAlphaOfLength(5), req));
        assertThat(exception.getMessage(),
            equalTo("rollover target is a [concrete index] but one of [alias,data_stream] was expected"));
        final String aliasName = randomAlphaOfLength(5);
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, aliasName, randomAlphaOfLength(5), req)
        );
        assertThat(exception.getMessage(), equalTo("rollover target [" + aliasName + "] does not exist"));
        MetadataRolloverService.validate(metadata, aliasWithWriteIndex, randomAlphaOfLength(5), req);
    }

    public void testDataStreamValidation() throws IOException {
        Metadata.Builder md = Metadata.builder();
        DataStream randomDataStream = DataStreamTests.randomInstance();
        for (Index index : randomDataStream.getIndices()) {
            md.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
        }
        md.put(randomDataStream);
        Metadata metadata = md.build();
        CreateIndexRequest req = new CreateIndexRequest();

        MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, req);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, randomDataStream.getName(), randomAlphaOfLength(5), req));
        assertThat(exception.getMessage(),
            equalTo("new index name may not be specified when rolling over a data stream"));

        CreateIndexRequest aliasReq = new CreateIndexRequest().alias(new Alias("no_aliases_permitted"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, aliasReq));
        assertThat(exception.getMessage(),
            equalTo("aliases, mappings, and index settings may not be specified when rolling over a data stream"));

        String mapping = Strings.toString(JsonXContent.contentBuilder().startObject().startObject("_doc").endObject().endObject());
        CreateIndexRequest mappingReq = new CreateIndexRequest().mapping(mapping);
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, mappingReq));
        assertThat(exception.getMessage(),
            equalTo("aliases, mappings, and index settings may not be specified when rolling over a data stream"));

        CreateIndexRequest settingReq = new CreateIndexRequest().settings(Settings.builder().put("foo", "bar"));
        exception = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, settingReq));
        assertThat(exception.getMessage(),
            equalTo("aliases, mappings, and index settings may not be specified when rolling over a data stream"));
    }

    public void testGenerateRolloverIndexName() {
        String invalidIndexName = randomAlphaOfLength(10) + "A";
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.generateRolloverIndexName(invalidIndexName, indexNameExpressionResolver));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAlphaOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(MetadataRolloverService.generateRolloverIndexName(indexEndingInNumbers, indexNameExpressionResolver),
            equalTo(indexPrefix + "-" + String.format(Locale.ROOT, "%06d", num + 1)));
        assertThat(MetadataRolloverService.generateRolloverIndexName("index-name-1", indexNameExpressionResolver),
            equalTo("index-name-000002"));
        assertThat(MetadataRolloverService.generateRolloverIndexName("index-name-2", indexNameExpressionResolver),
            equalTo("index-name-000003"));
        assertEquals( "<index-name-{now/d}-000002>", MetadataRolloverService.generateRolloverIndexName("<index-name-{now/d}-1>",
            indexNameExpressionResolver));
    }

    public void testCreateIndexRequest() {
        String alias = randomAlphaOfLength(10);
        String rolloverIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest =
            MetadataRolloverService.prepareCreateIndexRequest(rolloverIndex, rolloverIndex, rolloverRequest.getCreateIndexRequest());
        assertThat(createIndexRequest.settings(), equalTo(settings));
        assertThat(createIndexRequest.index(), equalTo(rolloverIndex));
        assertThat(createIndexRequest.cause(), equalTo("rollover_index"));
    }

    public void testCreateIndexRequestForDataStream() {
        DataStream dataStream = DataStreamTests.randomInstance();
        final String newWriteIndexName = DataStream.getBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
        final RolloverRequest rolloverRequest = new RolloverRequest(dataStream.getName(), randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest =
            MetadataRolloverService.prepareDataStreamCreateIndexRequest(newWriteIndexName, rolloverRequest.getCreateIndexRequest());
        for (String settingKey : settings.keySet()) {
            assertThat(settings.get(settingKey), equalTo(createIndexRequest.settings().get(settingKey)));
        }
        assertThat(createIndexRequest.settings().get("index.hidden"), equalTo("true"));
        assertThat(createIndexRequest.index(), equalTo(newWriteIndexName));
        assertThat(createIndexRequest.cause(), equalTo("rollover_data_stream"));
    }

    public void testRejectDuplicateAlias() {
        final IndexTemplateMetadata template = IndexTemplateMetadata.builder("test-template")
            .patterns(Arrays.asList("foo-*", "bar-*"))
            .putAlias(AliasMetadata.builder("foo-write")).putAlias(AliasMetadata.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final Metadata metadata = Metadata.builder().put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, randomBoolean()));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testRejectDuplicateAliasV2() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final IndexTemplateV2 template = new IndexTemplateV2(Arrays.asList("foo-*", "bar-*"), new Template(null, null, aliases),
            null, null, null, null, null);

        final Metadata metadata = Metadata.builder().put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("test-template", template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, randomBoolean()));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testRejectDuplicateAliasV2UsingComponentTemplates() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final ComponentTemplate ct = new ComponentTemplate(new Template(null, null, aliases), null, null);
        final IndexTemplateV2 template = new IndexTemplateV2(Arrays.asList("foo-*", "bar-*"), null,
            Collections.singletonList("ct"), null, null, null, null);

        final Metadata metadata = Metadata.builder().put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("ct", ct)
            .put("test-template", template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, randomBoolean()));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedTemplates() {
        final IndexTemplateMetadata template = IndexTemplateMetadata.builder("test-template")
            .patterns(Collections.singletonList("*"))
            .putAlias(AliasMetadata.builder("foo-write")).putAlias(AliasMetadata.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final Metadata metadata = Metadata.builder().put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false).put(template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, randomFrom(Boolean.FALSE, null)));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedV2Templates() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final IndexTemplateV2 template = new IndexTemplateV2(Collections.singletonList("*"), new Template(null, null, aliases),
            null, null, null, null, null);

        final Metadata metadata = Metadata.builder().put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("test-template", template).build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, randomFrom(Boolean.FALSE, null)));
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedV2ComponentTemplates() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final ComponentTemplate ct = new ComponentTemplate(new Template(null, null, aliases), null, null);
        final IndexTemplateV2 template = new IndexTemplateV2(Collections.singletonList("*"), null,
            Collections.singletonList("ct"), null, null, null, null);

        final Metadata metadata = Metadata.builder().put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("ct", ct)
            .put("test-template", template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(metadata, indexName, aliasName, randomFrom(Boolean.FALSE, null)));
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
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder(sourceIndexName)
            .putAlias(AliasMetadata.builder(aliasName).writeIndex(true).build()).settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(1);
        final ClusterState clusterState =
            ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata)).build();

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

            MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(Settings.EMPTY,
                clusterService, indicesService, allocationService, null, env, null, testThreadPool, null, Collections.emptyList(), false);
            MetadataIndexAliasesService indexAliasesService = new MetadataIndexAliasesService(clusterService, indicesService,
                new AliasValidator(), null, xContentRegistry());
            MetadataRolloverService rolloverService = new MetadataRolloverService(testThreadPool, createIndexService, indexAliasesService,
                mockIndexNameExpressionResolver);

            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            String newIndexName = randomBoolean() ? "logs-index-9" : null;
            int numberOfShards = randomIntBetween(1, 5);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
            createIndexRequest.settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards));

            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult =
                rolloverService.rolloverClusterState(clusterState, aliasName, newIndexName, createIndexRequest, metConditions,
                    randomBoolean());
            long after = testThreadPool.absoluteTimeInMillis();

            newIndexName = newIndexName == null ? indexPrefix + "2" : newIndexName;
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName);
            assertEquals(newIndexName, rolloverResult.rolloverIndexName);
            Metadata rolloverMetadata = rolloverResult.clusterState.metadata();
            assertEquals(2, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);
            assertThat(rolloverIndexMetadata.getNumberOfShards(), equalTo(numberOfShards));

            IndexAbstraction alias = rolloverMetadata.getIndicesLookup().get(aliasName);
            assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
            assertThat(alias.getIndices(), hasSize(2));
            assertThat(alias.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName)));
            assertThat(alias.getIndices(), hasItem(rolloverIndexMetadata));
            assertThat(alias.getWriteIndex(), equalTo(rolloverIndexMetadata));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(aliasName);
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverClusterStateForDataStream() throws Exception {
        final DataStream dataStream = DataStreamTests.randomInstance();
        Metadata.Builder builder = Metadata.builder();
        for (Index index : dataStream.getIndices()) {
            builder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
        }
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(builder).build();

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

            MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(Settings.EMPTY,
                clusterService, indicesService, allocationService, null, env, null, testThreadPool, null, Collections.emptyList(), false);
            MetadataIndexAliasesService indexAliasesService = new MetadataIndexAliasesService(clusterService, indicesService,
                new AliasValidator(), null, xContentRegistry());
            MetadataRolloverService rolloverService = new MetadataRolloverService(testThreadPool, createIndexService, indexAliasesService,
                mockIndexNameExpressionResolver);

            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult =
                rolloverService.rolloverClusterState(clusterState, dataStream.getName(), null, createIndexRequest, metConditions,
                    randomBoolean());
            long after = testThreadPool.absoluteTimeInMillis();

            String sourceIndexName = DataStream.getBackingIndexName(dataStream.getName(), dataStream.getGeneration());
            String newIndexName = DataStream.getBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName);
            assertEquals(newIndexName, rolloverResult.rolloverIndexName);
            Metadata rolloverMetadata = rolloverResult.clusterState.metadata();
            assertEquals(dataStream.getIndices().size() + 1, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);

            IndexAbstraction ds = rolloverMetadata.getIndicesLookup().get(dataStream.getName());
            assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(ds.getIndices(), hasSize(dataStream.getIndices().size() + 1));
            assertThat(ds.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName)));
            assertThat(ds.getIndices(), hasItem(rolloverIndexMetadata));
            assertThat(ds.getWriteIndex(), equalTo(rolloverIndexMetadata));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(dataStream.getName());
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));
        } finally {
            testThreadPool.shutdown();
        }
    }

    private IndicesService mockIndicesServices() throws Exception {
        /*
         * Throws Exception because Eclipse uses the lower bound for
         * CheckedFunction's exception type so it thinks the "when" call
         * can throw Exception. javac seems to be ok inferring something
         * else.
         */
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.withTempIndexService(any(IndexMetadata.class), any(CheckedFunction.class)))
            .then(invocationOnMock -> {
                IndexService indexService = mock(IndexService.class);
                IndexMetadata indexMetadata = (IndexMetadata) invocationOnMock.getArguments()[0];
                when(indexService.index()).thenReturn(indexMetadata.getIndex());
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

    private static IndexMetadata createMetadata(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetadata.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }
}
