/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.indices.resolution;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class IndexNameExpressionResolverBenchmark {

    private static final String DATA_STREAM_PREFIX = "my-ds-";
    private static final String INDEX_PREFIX = "my-index-";

    @Param(
        {
            // # data streams | # indices
            "              1000|        100",
            "              5000|        500",
            "             10000|       1000" }
    )
    public String resourceMix = "100|10";

    @Setup
    public void setUp() {
        final String[] params = resourceMix.split("\\|");

        int numDataStreams = toInt(params[0]);
        int numIndices = toInt(params[1]);

        ProjectMetadata.Builder pmb = ProjectMetadata.builder(ProjectId.DEFAULT);
        String[] indices = new String[numIndices + numDataStreams * (numIndices + 1)];
        int position = 0;
        for (int i = 1; i <= numIndices; i++) {
            String indexName = INDEX_PREFIX + i;
            createIndexMetadata(indexName, pmb);
            indices[position++] = indexName;
        }

        for (int i = 1; i <= numDataStreams; i++) {
            String dataStreamName = DATA_STREAM_PREFIX + i;
            List<Index> backingIndices = new ArrayList<>();
            for (int j = 1; j <= numIndices; j++) {
                String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, j);
                backingIndices.add(createIndexMetadata(backingIndexName, pmb).getIndex());
                indices[position++] = backingIndexName;
            }
            indices[position++] = dataStreamName;
            pmb.put(DataStream.builder(dataStreamName, backingIndices).build());
        }
        int mid = indices.length / 2;
        clusterState = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(pmb).build();
        resolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            new SystemIndices(List.of()),
            DefaultProjectResolver.INSTANCE
        );
        indexListRequest = new Request(IndicesOptions.lenientExpandOpenHidden(), indices);
        starRequest = new Request(IndicesOptions.lenientExpandOpenHidden(), "*");
        String[] mixed = indices.clone();
        mixed[mid] = "my-*";
        mixedRequest = new Request(IndicesOptions.lenientExpandOpenHidden(), mixed);
    }

    private IndexMetadata createIndexMetadata(String indexName, ProjectMetadata.Builder pmb) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        pmb.put(indexMetadata, false);
        return indexMetadata;
    }

    private IndexNameExpressionResolver resolver;
    private ClusterState clusterState;
    private Request starRequest;
    private Request indexListRequest;
    private Request mixedRequest;

    @Benchmark
    public String[] resolveResourcesListToConcreteIndices() {
        return resolver.concreteIndexNames(clusterState, indexListRequest);
    }

    @Benchmark
    public String[] resolveAllStarToConcreteIndices() {
        return resolver.concreteIndexNames(clusterState, starRequest);
    }

    @Benchmark
    public String[] resolveMixedConcreteIndices() {
        return resolver.concreteIndexNames(clusterState, mixedRequest);
    }

    private int toInt(String v) {
        return Integer.parseInt(v.trim());
    }

    record Request(IndicesOptions indicesOptions, String... indices) implements IndicesRequest {

    }
}
