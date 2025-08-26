/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycleTests.randomDownsampling;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.indices.ShardLimitValidatorTests.createTestShardLimitService;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * We test the warnings added when user configured retention exceeds the global retention in this test,
 * so we can disable the warning check without impacting all the other test cases
 */
public class DataStreamLifecycleWithRetentionWarningsTests extends ESTestCase {
    @Override
    protected boolean enableWarningsCheck() {
        // this test expects warnings
        return false;
    }

    public void testNoHeaderWarning() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        DataStreamLifecycle noRetentionLifecycle = DataStreamLifecycle.dataLifecycleBuilder().downsampling(randomDownsampling()).build();
        noRetentionLifecycle.addWarningHeaderIfDataRetentionNotEffective(null, randomBoolean());
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.isEmpty(), is(true));

        TimeValue dataStreamRetention = TimeValue.timeValueDays(randomIntBetween(5, 100));
        DataStreamLifecycle lifecycleWithRetention = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(dataStreamRetention)
            .downsampling(randomDownsampling())
            .build();
        DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(
            TimeValue.timeValueDays(2),
            TimeValue.timeValueDays(dataStreamRetention.days() + randomIntBetween(1, 5))
        );
        lifecycleWithRetention.addWarningHeaderIfDataRetentionNotEffective(globalRetention, false);
        responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.isEmpty(), is(true));
    }

    public void testDefaultRetentionHeaderWarning() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        DataStreamLifecycle noRetentionLifecycle = DataStreamLifecycle.dataLifecycleBuilder().downsampling(randomDownsampling()).build();
        DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(
            randomTimeValue(2, 10, TimeUnit.DAYS),
            randomBoolean() ? null : TimeValue.timeValueDays(20)
        );
        noRetentionLifecycle.addWarningHeaderIfDataRetentionNotEffective(globalRetention, false);
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(1));
        assertThat(
            responseHeaders.get("Warning").get(0),
            containsString(
                "Not providing a retention is not allowed for this project. The default retention of ["
                    + globalRetention.defaultRetention().getStringRep()
                    + "] will be applied."
            )
        );
    }

    public void testMaxRetentionHeaderWarning() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        TimeValue maxRetention = randomTimeValue(2, 100, TimeUnit.DAYS);
        DataStreamLifecycle lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(randomBoolean() ? null : TimeValue.timeValueDays(maxRetention.days() + 1))
            .downsampling(randomDownsampling())
            .build();
        DataStreamGlobalRetention globalRetention = new DataStreamGlobalRetention(null, maxRetention);
        lifecycle.addWarningHeaderIfDataRetentionNotEffective(globalRetention, false);
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(1));
        String userRetentionPart = lifecycle.dataRetention() == null
            ? "Not providing a retention is not allowed for this project."
            : "The retention provided ["
                + lifecycle.dataRetention().getStringRep()
                + "] is exceeding the max allowed data retention of this project ["
                + maxRetention.getStringRep()
                + "].";
        assertThat(
            responseHeaders.get("Warning").get(0),
            containsString(userRetentionPart + " The max retention of [" + maxRetention.getStringRep() + "] will be applied")
        );
    }

    public void testUpdatingLifecycleOnADataStream() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        String dataStream = randomAlphaOfLength(5);
        TimeValue defaultRetention = randomTimeValue(2, 100, TimeUnit.DAYS);
        ClusterState before = ClusterState.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(List.of(new Tuple<>(dataStream, 2)), List.of())
        ).build();

        Settings settingsWithDefaultRetention = builder().put(
            DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(),
            defaultRetention
        ).build();

        MetadataDataStreamsService metadataDataStreamsService = new MetadataDataStreamsService(
            mock(ClusterService.class),
            mock(IndicesService.class),
            DataStreamGlobalRetentionSettings.create(ClusterSettings.createBuiltInClusterSettings(settingsWithDefaultRetention))
        );

        ClusterState after = metadataDataStreamsService.updateDataLifecycle(
            before,
            List.of(dataStream),
            DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
        );
        DataStream updatedDataStream = after.metadata().dataStreams().get(dataStream);
        assertNotNull(updatedDataStream);
        assertThat(updatedDataStream.getDataLifecycle(), equalTo(DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE));
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(1));
        assertThat(
            responseHeaders.get("Warning").get(0),
            containsString(
                "Not providing a retention is not allowed for this project. The default retention of ["
                    + defaultRetention.getStringRep()
                    + "] will be applied."
            )
        );
    }

    public void testValidateLifecycleIndexTemplateWithWarning() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        TimeValue defaultRetention = randomTimeValue(2, 100, TimeUnit.DAYS);
        MetadataIndexTemplateService.validateLifecycle(
            Metadata.builder().build(),
            randomAlphaOfLength(10),
            ComposableIndexTemplate.builder()
                .template(Template.builder().lifecycle(DataStreamLifecycle.Template.DATA_DEFAULT))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .indexPatterns(List.of(randomAlphaOfLength(10)))
                .build(),
            new DataStreamGlobalRetention(defaultRetention, null)
        );
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(1));
        assertThat(
            responseHeaders.get("Warning").get(0),
            containsString(
                "Not providing a retention is not allowed for this project. The default retention of ["
                    + defaultRetention.getStringRep()
                    + "] will be applied."
            )
        );
    }

    public void testValidateInternalDataStreamRetentionWithoutWarning() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        TimeValue defaultRetention = randomTimeValue(2, 100, TimeUnit.DAYS);
        MetadataIndexTemplateService.validateLifecycle(
            Metadata.builder().build(),
            randomAlphaOfLength(10),
            ComposableIndexTemplate.builder()
                .template(Template.builder().lifecycle(DataStreamLifecycle.Template.DATA_DEFAULT))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .indexPatterns(List.of("." + randomAlphaOfLength(10)))
                .build(),
            new DataStreamGlobalRetention(defaultRetention, null)
        );
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(0));
    }

    /**
     * Make sure we still take into account component templates during validation (and not just the index template).
     */
    public void testValidateLifecycleComponentTemplateWithWarning() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        TimeValue defaultRetention = randomTimeValue(2, 100, TimeUnit.DAYS);
        MetadataIndexTemplateService.validateLifecycle(
            Metadata.builder()
                .componentTemplates(
                    Map.of(
                        "component-template",
                        new ComponentTemplate(
                            Template.builder()
                                .lifecycle(DataStreamLifecycle.dataLifecycleBuilder().dataRetention(randomTimeValue(2, 100, TimeUnit.DAYS)))
                                .build(),
                            null,
                            null
                        )
                    )
                )
                .build(),
            randomAlphaOfLength(10),
            ComposableIndexTemplate.builder()
                .template(Template.builder().lifecycle(DataStreamLifecycle.Template.DATA_DEFAULT))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .indexPatterns(List.of(randomAlphaOfLength(10)))
                .componentTemplates(List.of("component-template"))
                .build(),
            new DataStreamGlobalRetention(defaultRetention, null)
        );
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(0));
    }

    public void testValidateLifecycleInComponentTemplate() throws Exception {
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.createIndex(any(), any(), eq(false))).thenReturn(indexService);
        when(indexService.index()).thenReturn(new Index(randomAlphaOfLength(10), randomUUID()));
        ClusterService clusterService = mock(ClusterService.class);
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesService,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000)),
            new Environment(builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(), null),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            null,
            xContentRegistry(),
            EmptySystemIndices.INSTANCE,
            true,
            new IndexSettingProviders(Set.of())
        );
        TimeValue defaultRetention = randomTimeValue(2, 100, TimeUnit.DAYS);
        Settings settingsWithDefaultRetention = Settings.builder()
            .put(DataStreamGlobalRetentionSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), defaultRetention)
            .build();
        ClusterState state = ClusterState.EMPTY_STATE;
        MetadataIndexTemplateService metadataIndexTemplateService = new MetadataIndexTemplateService(
            clusterService,
            createIndexService,
            indicesService,
            new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS),
            xContentRegistry(),
            EmptySystemIndices.INSTANCE,
            new IndexSettingProviders(Set.of()),
            DataStreamGlobalRetentionSettings.create(ClusterSettings.createBuiltInClusterSettings(settingsWithDefaultRetention))
        );

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        Template template = Template.builder()
            .settings(ComponentTemplateTests.randomSettings())
            .aliases(ComponentTemplateTests.randomAliases())
            .lifecycle(DataStreamLifecycle.Template.DATA_DEFAULT)
            .build();
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());
        state = metadataIndexTemplateService.addComponentTemplate(state, false, "foo", componentTemplate);

        assertNotNull(state.metadata().componentTemplates().get("foo"));
        assertThat(state.metadata().componentTemplates().get("foo"), equalTo(componentTemplate));
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        assertThat(responseHeaders.size(), is(1));
        assertThat(
            responseHeaders.get("Warning").get(0),
            containsString(
                "Not providing a retention is not allowed for this project. The default retention of ["
                    + defaultRetention.getStringRep()
                    + "] will be applied."
            )
        );
    }
}
