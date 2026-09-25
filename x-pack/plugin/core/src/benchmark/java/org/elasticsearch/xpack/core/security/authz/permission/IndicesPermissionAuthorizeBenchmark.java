/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link IndicesPermission#authorize} for a bulk write into a data stream, the shape behind the
 * production OOM this code path was optimised for: a role carrying FLS and/or DLS, and a data stream with
 * many backing indices, each of which gets an entry in the resulting {@link IndicesAccessControl}.
 *
 * <p>{@code IndicesAccessControl} builds its per-index map lazily, so each benchmark method reads one entry
 * back. Without that read the expensive part never runs and the benchmark measures only resource resolution.
 *
 * <p>Run with {@code -prof gc} to see allocation per call ({@code gc.alloc.rate.norm}), which is the number
 * that tracks the memory side of this change.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class IndicesPermissionAuthorizeBenchmark {

    static {
        // IndicesPermission acquires an ES Logger; the forked JMH VM has no logging provider installed.
        BenchmarkLogging.configure();
    }

    public enum DlsFls {
        NONE,
        FLS,
        DLS,
        BOTH
    }

    private static final String DATA_STREAM = "logs-app";

    /** Backing indices in the data stream; 1500 is the size seen in the incident. */
    @Param({ "1", "100", "1500" })
    int backingIndices;

    /** Which document- and field-level restrictions the matching groups carry. */
    @Param({ "NONE", "FLS", "BOTH" })
    DlsFls dlsFls;

    /** Groups in the role that match the data stream, each with a distinct FLS grant and DLS query. */
    @Param({ "1", "5" })
    int matchingGroups;

    private IndicesPermission permission;
    private ProjectMetadata metadata;
    private FieldPermissionsCache fieldPermissionsCache;
    private Set<String> dataStreamRequest;
    private Set<String> backingIndexRequest;
    private String probeIndex;

    @Setup(Level.Trial)
    public void setup() {
        final List<IndexMetadata> indices = new ArrayList<>(backingIndices);
        for (int i = 1; i <= backingIndices; i++) {
            indices.add(backingIndex(DataStream.getDefaultBackingIndexName(DATA_STREAM, i)));
        }
        final ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        builder.put(DataStream.builder(DATA_STREAM, indices.stream().map(IndexMetadata::getIndex).toList()).build());
        for (IndexMetadata index : indices) {
            builder.put(index, false);
        }
        metadata = builder.build();

        final IndicesPermission.Builder permissionBuilder = new IndicesPermission.Builder(new RestrictedIndices(Automatons.EMPTY));
        for (int i = 0; i < matchingGroups; i++) {
            permissionBuilder.addGroup(IndexPrivilege.WRITE, fieldPermissions(i), query(i), false, "logs-*");
        }
        permission = permissionBuilder.build();

        fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        dataStreamRequest = Set.of(DATA_STREAM);
        probeIndex = indices.get(indices.size() - 1).getIndex().getName();
        backingIndexRequest = Set.of(probeIndex);
    }

    /**
     * The incident shape: the request names the data stream, so every backing index is granted implicitly and
     * receives an entry.
     */
    @Benchmark
    public IndicesAccessControl.IndexAccessControl authorizeDataStream() {
        return authorizeDataStreamAccessControl().getIndexPermissions(probeIndex);
    }

    /**
     * The per-shard-request shape: the request names one concrete backing index directly. Independent of
     * {@link #backingIndices}; measures the fixed cost of a single-index authorization.
     */
    @Benchmark
    public IndicesAccessControl.IndexAccessControl authorizeBackingIndexDirectly() {
        return permission.authorize(TransportShardBulkAction.ACTION_NAME, backingIndexRequest, metadata, fieldPermissionsCache)
            .getIndexPermissions(probeIndex);
    }

    IndicesAccessControl authorizeDataStreamAccessControl() {
        return permission.authorize(TransportShardBulkAction.ACTION_NAME, dataStreamRequest, metadata, fieldPermissionsCache);
    }

    List<String> backingIndexNames() {
        final List<String> names = new ArrayList<>(backingIndices);
        for (Index index : metadata.dataStreams().get(DATA_STREAM).getIndices()) {
            names.add(index.getName());
        }
        return names;
    }

    private FieldPermissions fieldPermissions(int group) {
        return switch (dlsFls) {
            case FLS, BOTH -> new FieldPermissions(
                new FieldPermissionsDefinition(new String[] { "@timestamp", "message", "field-" + group }, new String[0])
            );
            case NONE, DLS -> FieldPermissions.DEFAULT;
        };
    }

    private Set<BytesReference> query(int group) {
        return switch (dlsFls) {
            case DLS, BOTH -> Set.of(new BytesArray("{\"term\":{\"tenant\":\"tenant-" + group + "\"}}"));
            case NONE, FLS -> null;
        };
    }

    private static IndexMetadata backingIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).put("index.hidden", true))
            .state(IndexMetadata.State.OPEN)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    }
}
