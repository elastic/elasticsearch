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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DatafeedConfigReaderTests extends ESTestCase {

    private final String JOB_ID = "foo";

    private void mockProviderWithExpectedIds(DatafeedConfigProvider mockedProvider, String expression, SortedSet<String> datafeedIds) {
        doAnswer(invocationOnMock -> {
            ActionListener<SortedSet<String>> listener = (ActionListener<SortedSet<String>>) invocationOnMock.getArguments()[1];
            listener.onResponse(datafeedIds);
            return null;
        }).when(mockedProvider).expandDatafeedIdsWithoutMissingCheck(eq(expression), any());
    }

    public void testExpandDatafeedIds_SplitBetweenClusterStateAndIndex() {
        SortedSet<String> idsInIndex = new TreeSet<>();
        idsInIndex.add("index-df");
        DatafeedConfigProvider provider = mock(DatafeedConfigProvider.class);
        mockProviderWithExpectedIds(provider, "cs-df,index-df", idsInIndex);

        ClusterState clusterState = buildClusterStateWithJob(Collections.singletonList(createDatafeedConfig("cs-df", JOB_ID)));

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

        ClusterState clusterState = buildClusterStateWithJob(Collections.singletonList(createDatafeedConfig("df3", JOB_ID)));

        DatafeedConfigReader reader = new DatafeedConfigReader(provider);

        AtomicReference<SortedSet<String>> idsHolder = new AtomicReference<>();
        reader.expandDatafeedIds("_all", true, clusterState, ActionListener.wrap(
                idsHolder::set,
                e -> fail(e.getMessage())
        ));

        assertNotNull(idsHolder.get());
        assertThat(idsHolder.get(), contains("df1", "df2", "df3"));
    }

    private ClusterState buildClusterStateWithJob(List<DatafeedConfig> datafeeds) {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder(JOB_ID).build(), false);
        for (DatafeedConfig df : datafeeds) {
            mlMetadata.putDatafeed(df, Collections.emptyMap());
        }

        return ClusterState.builder(new ClusterName("datafeedconfigreadertests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
    }

    private DatafeedConfig createDatafeedConfig(String id, String jobId) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(id, jobId);
        builder.setIndices(Collections.singletonList("beats*"));
        return builder.build();
    }
}
