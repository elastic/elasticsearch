/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
        final Settings settings = indexSettings(IndexVersion.current(), 1, 0).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        ).build();
        ProjectMetadata.Builder metadataBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(
                IndexMetadata.builder(index1)
                    .settings(settings)
                    .putAlias(AliasMetadata.builder(aliasWithWriteIndex))
                    .putAlias(AliasMetadata.builder(aliasWithNoWriteIndex).writeIndex(firstIsWriteIndex))
            );
        IndexMetadata.Builder indexTwoBuilder = IndexMetadata.builder(index2).settings(settings);
        if (firstIsWriteIndex == null) {
            indexTwoBuilder.putAlias(AliasMetadata.builder(aliasWithNoWriteIndex).writeIndex(randomFrom(false, null)));
        }
        metadataBuilder.put(indexTwoBuilder);
        ProjectMetadata metadata = metadataBuilder.build();
        CreateIndexRequest req = new CreateIndexRequest();

        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.validate(metadata, aliasWithNoWriteIndex, randomAlphaOfLength(5), req)
        );
        assertThat(exception.getMessage(), equalTo("rollover target [" + aliasWithNoWriteIndex + "] does not point to a write index"));
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.validate(metadata, randomFrom(index1, index2), randomAlphaOfLength(5), req)
        );
        assertThat(exception.getMessage(), equalTo("rollover target is a [concrete index] but one of [alias,data_stream] was expected"));
        final String aliasName = randomAlphaOfLength(5);
        exception = expectThrows(
            ResourceNotFoundException.class,
            () -> MetadataRolloverService.validate(metadata, aliasName, randomAlphaOfLength(5), req)
        );
        assertThat(exception.getMessage(), equalTo("rollover target [" + aliasName + "] does not exist"));
        MetadataRolloverService.validate(metadata, aliasWithWriteIndex, randomAlphaOfLength(5), req);
    }

    public void testDataStreamValidation() throws IOException {
        ProjectMetadata.Builder md = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream randomDataStream = DataStreamTestHelper.randomInstance(false);
        for (Index index : randomDataStream.getIndices()) {
            md.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
        }
        md.put(randomDataStream);
        ProjectMetadata metadata = md.build();
        CreateIndexRequest req = new CreateIndexRequest();

        MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, req);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.validate(metadata, randomDataStream.getName(), randomAlphaOfLength(5), req)
        );
        assertThat(exception.getMessage(), equalTo("new index name may not be specified when rolling over a data stream"));

        CreateIndexRequest aliasReq = new CreateIndexRequest().alias(new Alias("no_aliases_permitted"));
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, aliasReq)
        );
        assertThat(
            exception.getMessage(),
            equalTo("aliases, mappings, and index settings may not be specified when rolling over a data stream")
        );

        String mapping = Strings.toString(JsonXContent.contentBuilder().startObject().startObject("_doc").endObject().endObject());
        CreateIndexRequest mappingReq = new CreateIndexRequest().mapping(mapping);
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, mappingReq)
        );
        assertThat(
            exception.getMessage(),
            equalTo("aliases, mappings, and index settings may not be specified when rolling over a data stream")
        );

        CreateIndexRequest settingReq = new CreateIndexRequest().settings(Settings.builder().put("foo", "bar"));
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.validate(metadata, randomDataStream.getName(), null, settingReq)
        );
        assertThat(
            exception.getMessage(),
            equalTo("aliases, mappings, and index settings may not be specified when rolling over a data stream")
        );
    }

    public void testGenerateRolloverIndexName() {
        String invalidIndexName = randomAlphaOfLength(10) + "A";
        expectThrows(IllegalArgumentException.class, () -> MetadataRolloverService.generateRolloverIndexName(invalidIndexName));
        int num = randomIntBetween(0, 100);
        final String indexPrefix = randomAlphaOfLength(10);
        String indexEndingInNumbers = indexPrefix + "-" + num;
        assertThat(
            MetadataRolloverService.generateRolloverIndexName(indexEndingInNumbers),
            equalTo(indexPrefix + "-" + Strings.format("%06d", num + 1))
        );
        assertThat(MetadataRolloverService.generateRolloverIndexName("index-name-1"), equalTo("index-name-000002"));
        assertThat(MetadataRolloverService.generateRolloverIndexName("index-name-2"), equalTo("index-name-000003"));
        assertEquals("<index-name-{now/d}-000002>", MetadataRolloverService.generateRolloverIndexName("<index-name-{now/d}-1>"));
    }

    public void testCreateIndexRequest() {
        String alias = randomAlphaOfLength(10);
        String rolloverIndex = randomAlphaOfLength(10);
        final RolloverRequest rolloverRequest = new RolloverRequest(alias, randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = indexSettings(IndexVersion.current(), 1, 0).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        ).build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest = MetadataRolloverService.prepareCreateIndexRequest(
            randomProjectIdOrDefault(),
            rolloverIndex,
            rolloverIndex,
            rolloverRequest.getCreateIndexRequest()
        );
        assertThat(createIndexRequest.settings(), equalTo(settings));
        assertThat(createIndexRequest.index(), equalTo(rolloverIndex));
        assertThat(createIndexRequest.cause(), equalTo("rollover_index"));
    }

    public void testCreateIndexRequestForDataStream() {
        DataStream dataStream = DataStreamTestHelper.randomInstance();
        final String newWriteIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
        final RolloverRequest rolloverRequest = new RolloverRequest(dataStream.getName(), randomAlphaOfLength(10));
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        rolloverRequest.getCreateIndexRequest().waitForActiveShards(activeShardCount);
        final Settings settings = indexSettings(IndexVersion.current(), 1, 0).put(
            IndexMetadata.SETTING_INDEX_UUID,
            UUIDs.randomBase64UUID()
        ).build();
        rolloverRequest.getCreateIndexRequest().settings(settings);
        final CreateIndexClusterStateUpdateRequest createIndexRequest = MetadataRolloverService.prepareDataStreamCreateIndexRequest(
            randomProjectIdOrDefault(),
            dataStream.getName(),
            newWriteIndexName,
            rolloverRequest.getCreateIndexRequest(),
            null,
            Instant.now()
        );
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
            .putAlias(AliasMetadata.builder("foo-write"))
            .putAlias(AliasMetadata.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put(template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testRejectDuplicateAliasV2() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(Arrays.asList("foo-*", "bar-*"))
            .template(new Template(null, null, aliases))
            .build();

        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("test-template", template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testRejectDuplicateAliasV2UsingComponentTemplates() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final ComponentTemplate ct = new ComponentTemplate(new Template(null, null, aliases), null, null);
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(Arrays.asList("foo-*", "bar-*"))
            .componentTemplates(Collections.singletonList("ct"))
            .build();

        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("ct", ct)
            .put("test-template", template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, randomBoolean())
        );
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testRolloverDoesntRejectOperationIfValidComposableTemplateOverridesLegacyTemplate() {
        final IndexTemplateMetadata legacyTemplate = IndexTemplateMetadata.builder("legacy-template")
            .patterns(Arrays.asList("foo-*", "bar-*"))
            .putAlias(AliasMetadata.builder("foo-write"))
            .putAlias(AliasMetadata.builder("bar-write").writeIndex(randomBoolean()))
            .build();

        // v2 template overrides the v1 template and does not define the rollover aliases
        final ComposableIndexTemplate composableTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(Arrays.asList("foo-*", "bar-*"))
            .template(new Template(null, null, null))
            .build();

        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put(legacyTemplate)
            .put("composable-template", composableTemplate)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // the valid v2 template takes priority over the v1 template so the validation should not throw any exception
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, randomBoolean());
    }

    public void testHiddenAffectsResolvedTemplates() {
        final IndexTemplateMetadata template = IndexTemplateMetadata.builder("test-template")
            .patterns(Collections.singletonList("*"))
            .putAlias(AliasMetadata.builder("foo-write"))
            .putAlias(AliasMetadata.builder("bar-write").writeIndex(randomBoolean()))
            .build();
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put(template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(
                projectMetadata,
                indexName,
                aliasName,
                randomFrom(Boolean.FALSE, null)
            )
        );
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedV2Templates() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList("*"))
            .template(new Template(null, null, aliases))
            .build();

        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("test-template", template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(
                projectMetadata,
                indexName,
                aliasName,
                randomFrom(Boolean.FALSE, null)
            )
        );
        assertThat(ex.getMessage(), containsString("index template [test-template]"));
    }

    public void testHiddenAffectsResolvedV2ComponentTemplates() {
        Map<String, AliasMetadata> aliases = new HashMap<>();
        aliases.put("foo-write", AliasMetadata.builder("foo-write").build());
        aliases.put("bar-write", AliasMetadata.builder("bar-write").writeIndex(randomBoolean()).build());
        final ComponentTemplate ct = new ComponentTemplate(new Template(null, null, aliases), null, null);
        final ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(Collections.singletonList("*"))
            .componentTemplates(Collections.singletonList("ct"))
            .build();

        final ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(createMetadata(randomAlphaOfLengthBetween(5, 7)), false)
            .put("ct", ct)
            .put("test-template", template)
            .build();
        String indexName = randomFrom("foo-123", "bar-xyz");
        String aliasName = randomFrom("foo-write", "bar-write");

        // hidden shouldn't throw
        MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(projectMetadata, indexName, aliasName, Boolean.TRUE);
        // not hidden will throw
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataRolloverService.checkNoDuplicatedAliasInIndexTemplate(
                projectMetadata,
                indexName,
                aliasName,
                randomFrom(Boolean.FALSE, null)
            )
        );
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
            .putAlias(AliasMetadata.builder(aliasName).writeIndex(true).build())
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final var projectId = randomProjectIdOrDefault();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(projectId).put(indexMetadata))
            .build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                null,
                testThreadPool,
                Set.of(),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );

            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            String newIndexName = randomBoolean() ? "logs-index-9" : null;
            int numberOfShards = randomIntBetween(1, 5);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
            createIndexRequest.settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards));

            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                aliasName,
                newIndexName,
                createIndexRequest,
                metConditions,
                Instant.now(),
                randomBoolean(),
                false,
                null,
                null,
                false
            );
            long after = testThreadPool.absoluteTimeInMillis();

            newIndexName = newIndexName == null ? indexPrefix + "2" : newIndexName;
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(2, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);
            assertThat(rolloverIndexMetadata.getNumberOfShards(), equalTo(numberOfShards));

            IndexAbstraction alias = rolloverMetadata.getIndicesLookup().get(aliasName);
            assertThat(alias.getType(), equalTo(IndexAbstraction.Type.ALIAS));
            assertThat(alias.getIndices(), hasSize(2));
            assertThat(alias.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName).getIndex()));
            assertThat(alias.getIndices(), hasItem(rolloverIndexMetadata.getIndex()));
            assertThat(alias.getWriteIndex(), equalTo(rolloverIndexMetadata.getIndex()));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(aliasName);
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));

            for (String metric : MetadataRolloverService.AUTO_SHARDING_METRIC_NAMES.values()) {
                assertThat(telemetryPlugin.getLongCounterMeasurement(metric), empty());
            }
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverClusterStateForDataStream() throws Exception {
        final DataStream dataStream = DataStreamTestHelper.randomInstance()
            // ensure no replicate data stream
            .promoteDataStream();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName() + "*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        for (Index index : dataStream.getIndices()) {
            builder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
        }
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );

            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

            String sourceIndexName = dataStream.getWriteIndex().getName();
            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                Instant.now(),
                randomBoolean(),
                false,
                null,
                null,
                false
            );
            long after = testThreadPool.absoluteTimeInMillis();
            Settings rolledOverIndexSettings = rolloverResult.clusterState()
                .projectState(projectId)
                .metadata()
                .index(rolloverResult.rolloverIndexName())
                .getSettings();
            Set<String> rolledOverIndexSettingNames = rolledOverIndexSettings.keySet();
            for (String settingName : dataStream.getEffectiveSettings(clusterState.projectState(projectId).metadata()).keySet()) {
                assertTrue(rolledOverIndexSettingNames.contains(settingName));
            }
            String newIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(dataStream.getIndices().size() + 1, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);

            IndexAbstraction ds = rolloverMetadata.getIndicesLookup().get(dataStream.getName());
            assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(ds.getIndices(), hasSize(dataStream.getIndices().size() + 1));
            assertThat(ds.getIndices(), hasItem(rolloverMetadata.index(sourceIndexName).getIndex()));
            assertThat(ds.getIndices(), hasItem(rolloverIndexMetadata.getIndex()));
            assertThat(ds.getWriteIndex(), equalTo(rolloverIndexMetadata.getIndex()));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(dataStream.getName());
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testRolloverClusterStateForDataStreamFailureStore() throws Exception {
        final DataStream dataStream = DataStreamTestHelper.randomInstance(true)
            // ensure no replicate data stream
            .promoteDataStream();
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream.getName() + "*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        builder.put("template", template);
        dataStream.getIndices().forEach(index -> builder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index)));
        dataStream.getFailureIndices().forEach(index -> builder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index)));
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        ThreadPool testThreadPool = new TestThreadPool(getTestName());
        try {
            MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
                dataStream,
                testThreadPool,
                Set.of(),
                xContentRegistry(),
                telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
            );

            MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
            List<Condition<?>> metConditions = Collections.singletonList(condition);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

            long before = testThreadPool.absoluteTimeInMillis();
            MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                Instant.now(),
                randomBoolean(),
                false,
                null,
                null,
                true
            );
            long after = testThreadPool.absoluteTimeInMillis();
            Settings rolledOverIndexSettings = rolloverResult.clusterState()
                .projectState(projectId)
                .metadata()
                .index(rolloverResult.rolloverIndexName())
                .getSettings();
            Set<String> rolledOverIndexSettingNames = rolledOverIndexSettings.keySet();
            for (String settingName : dataStream.getSettings().keySet()) {
                assertFalse(rolledOverIndexSettingNames.contains(settingName));
            }
            var epochMillis = System.currentTimeMillis();
            String sourceIndexName = DataStream.getDefaultFailureStoreName(dataStream.getName(), dataStream.getGeneration(), epochMillis);
            String newIndexName = DataStream.getDefaultFailureStoreName(dataStream.getName(), dataStream.getGeneration() + 1, epochMillis);
            assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
            assertEquals(newIndexName, rolloverResult.rolloverIndexName());
            ProjectMetadata rolloverMetadata = rolloverResult.clusterState().metadata().getProject(projectId);
            assertEquals(dataStream.getIndices().size() + dataStream.getFailureIndices().size() + 1, rolloverMetadata.indices().size());
            IndexMetadata rolloverIndexMetadata = rolloverMetadata.index(newIndexName);

            var ds = (DataStream) rolloverMetadata.getIndicesLookup().get(dataStream.getName());
            assertThat(ds.getType(), equalTo(IndexAbstraction.Type.DATA_STREAM));
            assertThat(ds.getIndices(), hasSize(dataStream.getIndices().size()));
            assertThat(ds.getFailureIndices(), hasSize(dataStream.getFailureIndices().size() + 1));
            assertThat(ds.getFailureIndices(), hasItem(rolloverMetadata.index(sourceIndexName).getIndex()));
            assertThat(ds.getFailureIndices(), hasItem(rolloverIndexMetadata.getIndex()));
            assertThat(ds.getWriteFailureIndex(), equalTo(rolloverIndexMetadata.getIndex()));

            RolloverInfo info = rolloverMetadata.index(sourceIndexName).getRolloverInfos().get(dataStream.getName());
            assertThat(info.getTime(), lessThanOrEqualTo(after));
            assertThat(info.getTime(), greaterThanOrEqualTo(before));
            assertThat(info.getMetConditions(), hasSize(1));
            assertThat(info.getMetConditions().get(0).value(), equalTo(condition.value()));
        } finally {
            testThreadPool.shutdown();
        }
    }

    public void testValidation() throws Exception {
        final String rolloverTarget;
        final String sourceIndexName;
        final String defaultRolloverIndexName;
        final boolean useDataStream = randomBoolean();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        boolean isFailureStoreRollover = false;
        if (useDataStream) {
            DataStream dataStream = DataStreamTestHelper.randomInstance()
                // ensure no replicate data stream
                .promoteDataStream();
            rolloverTarget = dataStream.getName();
            if (dataStream.isFailureStoreExplicitlyEnabled() && randomBoolean()) {
                sourceIndexName = dataStream.getWriteFailureIndex().getName();
                isFailureStoreRollover = true;
                defaultRolloverIndexName = DataStream.getDefaultFailureStoreName(
                    dataStream.getName(),
                    dataStream.getGeneration() + 1,
                    System.currentTimeMillis()
                );
            } else {
                sourceIndexName = dataStream.getIndices().get(dataStream.getIndices().size() - 1).getName();
                defaultRolloverIndexName = DataStream.getDefaultBackingIndexName(dataStream.getName(), dataStream.getGeneration() + 1);
            }
            ComposableIndexTemplate template = ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream.getName() + "*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build();
            builder.put("template", template);
            for (Index index : dataStream.getIndices()) {
                builder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
            }
            builder.put(dataStream);
        } else {
            String indexPrefix = "logs-index-00000";
            rolloverTarget = "logs-alias";
            sourceIndexName = indexPrefix + "1";
            defaultRolloverIndexName = indexPrefix + "2";
            final IndexMetadata.Builder indexMetadata = IndexMetadata.builder(sourceIndexName)
                .putAlias(AliasMetadata.builder(rolloverTarget).writeIndex(true).build())
                .settings(settings(IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1);
            builder.put(indexMetadata);
        }
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();

        MetadataCreateIndexService createIndexService = mock(MetadataCreateIndexService.class);
        MetadataIndexAliasesService metadataIndexAliasesService = mock(MetadataIndexAliasesService.class);
        ClusterService clusterService = mock(ClusterService.class);
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();
        MetadataRolloverService rolloverService = new MetadataRolloverService(
            null,
            createIndexService,
            metadataIndexAliasesService,
            EmptySystemIndices.INSTANCE,
            WriteLoadForecaster.DEFAULT,
            clusterService,
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
        );

        String newIndexName = useDataStream == false && randomBoolean() ? "logs-index-9" : null;

        MetadataRolloverService.RolloverResult rolloverResult = rolloverService.rolloverClusterState(
            clusterState.projectState(projectId),
            rolloverTarget,
            newIndexName,
            new CreateIndexRequest("_na_"),
            null,
            Instant.now(),
            randomBoolean(),
            true,
            null,
            null,
            isFailureStoreRollover
        );

        newIndexName = newIndexName == null ? defaultRolloverIndexName : newIndexName;
        assertEquals(sourceIndexName, rolloverResult.sourceIndexName());
        assertEquals(newIndexName, rolloverResult.rolloverIndexName());
        assertSame(rolloverResult.clusterState(), clusterState);
    }

    public void testRolloverClusterStateForDataStreamNoTemplate() throws Exception {
        final DataStream dataStream = DataStreamTestHelper.randomInstance();
        final var projectId = randomProjectIdOrDefault();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId);
        for (Index index : dataStream.getIndices()) {
            builder.put(DataStreamTestHelper.getIndexMetadataBuilderForIndex(index));
        }
        builder.put(dataStream);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(builder).build();
        final TestTelemetryPlugin telemetryPlugin = new TestTelemetryPlugin();

        ThreadPool testThreadPool = mock(ThreadPool.class);
        when(testThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        MetadataRolloverService rolloverService = DataStreamTestHelper.getMetadataRolloverService(
            dataStream,
            testThreadPool,
            Set.of(),
            xContentRegistry(),
            telemetryPlugin.getTelemetryProvider(Settings.EMPTY)
        );

        MaxDocsCondition condition = new MaxDocsCondition(randomNonNegativeLong());
        List<Condition<?>> metConditions = Collections.singletonList(condition);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> rolloverService.rolloverClusterState(
                clusterState.projectState(projectId),
                dataStream.getName(),
                null,
                createIndexRequest,
                metConditions,
                Instant.now(),
                false,
                randomBoolean(),
                null,
                null,
                false
            )
        );
        assertThat(e.getMessage(), equalTo("no matching index template found for data stream [" + dataStream.getName() + "]"));
    }

    private static IndexMetadata createMetadata(String indexName) {
        return IndexMetadata.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
            .build();
    }

}
