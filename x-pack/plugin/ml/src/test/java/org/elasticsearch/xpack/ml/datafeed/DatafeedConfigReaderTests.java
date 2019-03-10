/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DatafeedConfigReaderTests extends ESTestCase {

    private final String JOB_ID_FOO = "foo";

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @SuppressWarnings("unchecked")
    private void mockProviderWithExpectedIds(DatafeedConfigProvider mockedProvider, String expression, SortedSet<String> datafeedIds) {
        doAnswer(invocationOnMock -> {
            ActionListener<SortedSet<String>> listener = (ActionListener<SortedSet<String>>) invocationOnMock.getArguments()[1];
            listener.onResponse(datafeedIds);
            return null;
        }).when(mockedProvider).expandDatafeedIdsWithoutMissingCheck(eq(expression), any());
    }

    @SuppressWarnings("unchecked")
    private void mockProviderWithExpectedConfig(DatafeedConfigProvider mockedProvider, String expression,
                                                List<DatafeedConfig.Builder> datafeedConfigs) {
        doAnswer(invocationOnMock -> {
            ActionListener<List<DatafeedConfig.Builder>> listener =
                    (ActionListener<List<DatafeedConfig.Builder>>) invocationOnMock.getArguments()[1];
            listener.onResponse(datafeedConfigs);
            return null;
        }).when(mockedProvider).expandDatafeedConfigsWithoutMissingCheck(eq(expression), any());
    }

    public void testExpandDatafeedIds_SplitBetweenClusterStateAndIndex() {
        SortedSet<String> idsInIndex = new TreeSet<>();
        idsInIndex.add("index-df");
        DatafeedConfigProvider provider = mock(DatafeedConfigProvider.class);
        mockProviderWithExpectedIds(provider, "cs-df,index-df", idsInIndex);

        ClusterState clusterState = buildClusterStateWithJob(createDatafeedConfig("cs-df", JOB_ID_FOO));

        DatafeedConfigReader reader = new DatafeedConfigReader(provider);

        AtomicReference<SortedSet<String>> idsHolder = new AtomicReference<>();
        reader.expandDatafeedIds("cs-df,index-df", true, clusterState, ActionListener.wrap(
                idsHolder::set,
                e -> fail(e.getMessage())
        ));
        assertNotNull(idsHolder.get());
        assertThat(idsHolder.get(), contains("cs-df", "index-df"));

        mockProviderWithExpectedIds(provider, "cs-df", new TreeSet<>());
        reader.expandDatafeedIds("cs-df", true, clusterState, ActionListener.wrap(
                idsHolder::set,
                e -> assertNull(e)
        ));
        assertThat(idsHolder.get(), contains("cs-df"));

        idsInIndex.clear();
        idsInIndex.add("index-df");
        mockProviderWithExpectedIds(provider, "index-df", idsInIndex);
        reader.expandDatafeedIds("index-df", true, clusterState, ActionListener.wrap(
                idsHolder::set,
                e -> assertNull(e)
        ));
        assertThat(idsHolder.get(), contains("index-df"));
    }

    public void testExpandDatafeedIds_GivenAll() {
        SortedSet<String> idsInIndex = new TreeSet<>();
        idsInIndex.add("df1");
        idsInIndex.add("df2");
        DatafeedConfigProvider provider = mock(DatafeedConfigProvider.class);
        mockProviderWithExpectedIds(provider, "_all", idsInIndex);

        ClusterState clusterState = buildClusterStateWithJob(createDatafeedConfig("df3", JOB_ID_FOO));

        DatafeedConfigReader reader = new DatafeedConfigReader(provider);

        AtomicReference<SortedSet<String>> idsHolder = new AtomicReference<>();
        reader.expandDatafeedIds("_all", true, clusterState, ActionListener.wrap(
                idsHolder::set,
                e -> fail(e.getMessage())
        ));

        assertNotNull(idsHolder.get());
        assertThat(idsHolder.get(), contains("df1", "df2", "df3"));
    }

    public void testExpandDatafeedConfigs_SplitBetweenClusterStateAndIndex() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("job-a").build(), false);
        mlMetadata.putDatafeed(createDatafeedConfig("cs-df", "job-a"), Collections.emptyMap(), xContentRegistry());
        mlMetadata.putJob(buildJobBuilder("job-b").build(), false);
        mlMetadata.putDatafeed(createDatafeedConfig("ll-df", "job-b"), Collections.emptyMap(), xContentRegistry());

        ClusterState clusterState = ClusterState.builder(new ClusterName("datafeedconfigreadertests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();


        DatafeedConfig.Builder indexConfig = createDatafeedConfigBuilder("index-df", "job-c");
        DatafeedConfigProvider provider = mock(DatafeedConfigProvider.class);
        mockProviderWithExpectedConfig(provider, "_all", Collections.singletonList(indexConfig));

        DatafeedConfigReader reader = new DatafeedConfigReader(provider);

        AtomicReference<List<DatafeedConfig>> configHolder = new AtomicReference<>();
        reader.expandDatafeedConfigs("_all", true, clusterState, ActionListener.wrap(
                configHolder::set,
                e -> fail(e.getMessage())
        ));

        assertThat(configHolder.get(), hasSize(3));
        assertEquals("cs-df", configHolder.get().get(0).getId());
        assertEquals("index-df", configHolder.get().get(1).getId());
        assertEquals("ll-df", configHolder.get().get(2).getId());
    }

    public void testExpandDatafeedConfigs_DuplicateConfigReturnsClusterStateConfig() {
        // TODO
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("datafeed-in-clusterstate").build(), false);
        mlMetadata.putDatafeed(createDatafeedConfig("df1", "datafeed-in-clusterstate"), Collections.emptyMap(), xContentRegistry());
        ClusterState clusterState = ClusterState.builder(new ClusterName("datafeedconfigreadertests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        DatafeedConfig.Builder indexConfig1 = createDatafeedConfigBuilder("df1", "datafeed-in-index");
        DatafeedConfig.Builder indexConfig2 = createDatafeedConfigBuilder("df2", "job-c");
        DatafeedConfigProvider provider = mock(DatafeedConfigProvider.class);
        mockProviderWithExpectedConfig(provider, "_all", Arrays.asList(indexConfig1, indexConfig2));
        DatafeedConfigReader reader = new DatafeedConfigReader(provider);
        AtomicReference<List<DatafeedConfig>> configHolder = new AtomicReference<>();
        reader.expandDatafeedConfigs("_all", true, clusterState, ActionListener.wrap(
                configHolder::set,
                e -> fail(e.getMessage())
        ));
        assertThat(configHolder.get(), hasSize(2));
        assertEquals("df1", configHolder.get().get(0).getId());
        assertEquals("datafeed-in-clusterstate", configHolder.get().get(0).getJobId());
        assertEquals("df2", configHolder.get().get(1).getId());
    }

    private ClusterState buildClusterStateWithJob(DatafeedConfig datafeed) {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder(JOB_ID_FOO).build(), false);
        mlMetadata.putDatafeed(datafeed, Collections.emptyMap(), xContentRegistry());

        return ClusterState.builder(new ClusterName("datafeedconfigreadertests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
    }

    private DatafeedConfig.Builder createDatafeedConfigBuilder(String id, String jobId) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(id, jobId);
        builder.setIndices(Collections.singletonList("beats*"));
        return builder;
    }

    private DatafeedConfig createDatafeedConfig(String id, String jobId) {
        return createDatafeedConfigBuilder(id, jobId).build();
    }
}
