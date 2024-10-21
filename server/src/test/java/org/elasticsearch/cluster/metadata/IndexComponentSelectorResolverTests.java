/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.FailureIndexNotSupportedException;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFailureStore;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolverTests.setAllowSelectors;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * This test suite is complementary to {@link IndexNameExpressionResolverTests} and focuses on the selector resolution.
 */
public class IndexComponentSelectorResolverTests extends ESTestCase {

    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ThreadContext threadContext;
    private long epochMillis;

    private ThreadContext createThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }

    protected IndexNameExpressionResolver createIndexNameExpressionResolver(ThreadContext threadContext) {
        return TestIndexNameExpressionResolver.newInstance(threadContext);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = createThreadContext();
        indexNameExpressionResolver = createIndexNameExpressionResolver(threadContext);
        epochMillis = randomLongBetween(1580536800000L, 1583042400000L);
    }

    public void testDataStreamsWithFailureStoreWithSelector() {
        final String dataStreamName = "my-data-stream";
        final String dataStreamAlias = "my-alias";
        IndexMetadata index1 = createBackingIndex(dataStreamName, 1, epochMillis).build();
        IndexMetadata index2 = createBackingIndex(dataStreamName, 2, epochMillis).build();
        IndexMetadata failureIndex1 = createFailureStore(dataStreamName, 1, epochMillis).build();
        IndexMetadata failureIndex2 = createFailureStore(dataStreamName, 2, epochMillis).build();
        IndexMetadata otherIndex = IndexNameExpressionResolverTests.indexBuilder("my-other-index", Settings.EMPTY)
            .state(State.OPEN)
            .build();

        Metadata.Builder mdBuilder = Metadata.builder()
            .put(index1, false)
            .put(index2, false)
            .put(failureIndex1, false)
            .put(failureIndex2, false)
            .put(otherIndex, false)
            .put(
                newInstance(
                    dataStreamName,
                    List.of(index1.getIndex(), index2.getIndex()),
                    List.of(failureIndex1.getIndex(), failureIndex2.getIndex())
                )
            );
        mdBuilder.put(dataStreamAlias, dataStreamName, null, null);
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        // We will specify selectors on every test case, so the value of the default selector shouldn't matter
        IndicesOptions indicesOptions = IndicesOptions.builder()
            .selectorOptions(IndicesOptions.SelectorOptions.builder().defaultSelectors(randomFrom(IndexComponentSelector.values())))
            .build();

        // Test only data with an exact data stream name
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-data-stream::data");
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
        }

        // Test only data with an exact alias
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-alias::data");
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
        }

        // Test include failure store with an exact data stream name
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-data-stream::*");
            assertThat(result.length, equalTo(4));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
            assertThat(result[2].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 1, epochMillis)));
            assertThat(result[3].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 2, epochMillis)));
        }

        // Test include failure store with an exact alias
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-alias::*");
            assertThat(result.length, equalTo(4));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis)));
            assertThat(result[2].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 1, epochMillis)));
            assertThat(result[3].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 2, epochMillis)));
        }

        // Test only failure store with an exact data stream name
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-data-stream::failures");
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 2, epochMillis)));
        }

        // Test only failure store with an exact alias
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-alias::failures");
            assertThat(result.length, equalTo(2));
            assertThat(result[0].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 1, epochMillis)));
            assertThat(result[1].getName(), equalTo(DataStream.getDefaultFailureStoreName(dataStreamName, 2, epochMillis)));
        }

        // Test all supported with wildcard expression
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-*::*");
            assertThat(result.length, equalTo(5));
            List<String> indexNames = Arrays.stream(result).map(Index::getName).toList();
            assertThat(
                indexNames,
                containsInAnyOrder(
                    DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis),
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis),
                    otherIndex.getIndex().getName(),
                    DataStream.getDefaultFailureStoreName(dataStreamName, 1, epochMillis),
                    DataStream.getDefaultFailureStoreName(dataStreamName, 2, epochMillis)
                )
            );
        }

        // Test include only data with wildcard expression
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-*::data");
            assertThat(result.length, equalTo(3));
            List<String> indexNames = Arrays.stream(result).map(Index::getName).toList();
            assertThat(
                indexNames,
                containsInAnyOrder(
                    DataStream.getDefaultBackingIndexName(dataStreamName, 2, epochMillis),
                    DataStream.getDefaultBackingIndexName(dataStreamName, 1, epochMillis),
                    otherIndex.getIndex().getName()
                )
            );
        }

        // Test only failure store with wildcard expression
        {
            Index[] result = indexNameExpressionResolver.concreteIndices(state, indicesOptions, true, "my-*::failures");
            assertThat(result.length, equalTo(2));
            List<String> indexNames = Arrays.stream(result).map(Index::getName).toList();
            assertThat(
                indexNames,
                containsInAnyOrder(
                    DataStream.getDefaultFailureStoreName(dataStreamName, 2, epochMillis),
                    DataStream.getDefaultFailureStoreName(dataStreamName, 1, epochMillis)
                )
            );
        }

        // Test provide selector when selectors not allowed
        {
            IndicesOptions noSelectors = setAllowSelectors(indicesOptions, false);
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> indexNameExpressionResolver.concreteIndices(
                    state,
                    noSelectors,
                    true,
                    "my-data-stream::" + randomFrom(IndexComponentSelector.values()).getKey()
                )
            );
            assertThat(
                exception.getMessage(),
                containsString("Index component selectors are not supported in this context but found selector in expression")
            );
        }

        // Test throw an error when directly accessing the failure store when not supported
        {
            IndicesOptions noFailureIndices = IndicesOptions.builder(indicesOptions)
                .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder(indicesOptions.gatekeeperOptions()).allowFailureIndices(false))
                .build();
            FailureIndexNotSupportedException exception = expectThrows(
                FailureIndexNotSupportedException.class,
                () -> indexNameExpressionResolver.concreteIndices(state, noFailureIndices, true, failureIndex1.getIndex().getName())
            );
            assertThat(exception.getMessage(), containsString("failure index not supported"));
        }
    }
}
