/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.action.index.TransportIndexAction;
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
 * Benchmarks {@link LimitedRole#authorize} for a bulk write into a data stream: the API-key (and run-as) shape of
 * {@link IndicesPermissionAuthorizeBenchmark}. An API key's effective role is the owner's role limited by the key's own
 * role descriptors, so authorization runs {@link IndicesPermission#authorize} twice and composes the two results with
 * {@link IndicesAccessControl#limitIndicesAccessControl}, which calls
 * {@link IndicesAccessControl.IndexAccessControl#limitIndexAccessControl} once per common index.
 *
 * <p>The two inputs are shared per data stream, but composing them is per index: every backing index gets a new
 * {@code IndexAccessControl}, a new {@code DocumentPermissions} (re-copied {@code TreeSet}s whenever either side has DLS)
 * and, when either side carries FLS, a new {@code FieldPermissions}, whose constructor compiles a run automaton; when both
 * sides carry FLS that is preceded by an automaton intersection in {@code FieldPermissions#limitFieldPermissions}.
 *
 * <p>{@link #authorizeDataStreamOwnerOnly} is the reference: the owner role alone, the exact shape
 * {@code IndicesPermissionAuthorizeBenchmark#authorizeDataStream} measures. The difference between it and
 * {@link #authorizeDataStreamLimited} is the cost of composing the two results plus one extra authorization for the key
 * role. {@code gc.alloc.rate.norm} shows which part is per index.
 *
 * <pre>
 *   ./gradlew :x-pack:plugin:core:benchmark --args 'LimitedRoleAuthorizeBenchmark -prof gc'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class LimitedRoleAuthorizeBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    static final String DATA_STREAM = "logs-app";

    /** The bulk item's implied action, as in IndicesPermissionAuthorizeBenchmark. */
    private static final String ITEM_ACTION = TransportIndexAction.NAME + ":op_type/create";

    @Param({ "1", "100", "1500" })
    int backingIndices;

    /** The restrictions on the owner's role, granted on {@code logs-*}. */
    @Param({ "NONE", "FLS", "DLS", "BOTH" })
    IndicesPermissionAuthorizeBenchmark.DlsFls ownerDlsFls;

    /** The restrictions on the API key's role descriptor, granted on {@code logs-*}. */
    @Param({ "NONE", "FLS", "DLS", "BOTH" })
    IndicesPermissionAuthorizeBenchmark.DlsFls keyDlsFls;

    private Role ownerRole;
    private Role limitedRole;
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

        final RestrictedIndices restrictedIndices = new RestrictedIndices(Automatons.EMPTY);
        // Distinct field sets and queries on the two sides, so that neither side's restriction subsumes the other's and
        // the composition has to intersect fields and conjoin queries rather than short-circuit.
        ownerRole = Role.builder(restrictedIndices, "owner")
            .add(fieldPermissions(ownerDlsFls, "owner"), query(ownerDlsFls, "owner"), IndexPrivilege.WRITE, false, "logs-*")
            .build();
        final Role keyRole = Role.builder(restrictedIndices, "key")
            .add(fieldPermissions(keyDlsFls, "key"), query(keyDlsFls, "key"), IndexPrivilege.WRITE, false, "logs-*")
            .build();
        limitedRole = ownerRole.limitedBy(keyRole);

        fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        dataStreamRequest = Set.of(DATA_STREAM);
        probeIndex = indices.get(indices.size() - 1).getIndex().getName();
        backingIndexRequest = Set.of(probeIndex);
    }

    /** The reference: the owner's role alone. Same work as {@code IndicesPermissionAuthorizeBenchmark#authorizeDataStream}. */
    @Benchmark
    public IndicesAccessControl.IndexAccessControl authorizeDataStreamOwnerOnly() {
        return ownerRole.authorize(ITEM_ACTION, dataStreamRequest, metadata, fieldPermissionsCache).getIndexPermissions(probeIndex);
    }

    /**
     * The API-key shape: owner limited by key. Reading one entry forces both inner maps and the composed map, i.e. one
     * {@code limitIndexAccessControl} per backing index plus one for the data stream name.
     */
    @Benchmark
    public IndicesAccessControl.IndexAccessControl authorizeDataStreamLimited() {
        return authorizeDataStreamLimitedAccessControl().getIndexPermissions(probeIndex);
    }

    /** The per-shard hot path under an API key: one concrete index, composed once. Independent of {@link #backingIndices}. */
    @Benchmark
    public IndicesAccessControl.IndexAccessControl authorizeBackingIndexDirectlyLimited() {
        return limitedRole.authorize(ITEM_ACTION, backingIndexRequest, metadata, fieldPermissionsCache).getIndexPermissions(probeIndex);
    }

    IndicesAccessControl authorizeDataStreamLimitedAccessControl() {
        return limitedRole.authorize(ITEM_ACTION, dataStreamRequest, metadata, fieldPermissionsCache);
    }

    List<String> backingIndexNames() {
        final List<String> names = new ArrayList<>(backingIndices);
        for (Index index : metadata.dataStreams().get(DATA_STREAM).getIndices()) {
            names.add(index.getName());
        }
        return names;
    }

    private static FieldPermissions fieldPermissions(IndicesPermissionAuthorizeBenchmark.DlsFls dlsFls, String side) {
        return switch (dlsFls) {
            case FLS, BOTH -> new FieldPermissions(
                new FieldPermissionsDefinition(new String[] { "@timestamp", "message", "field-" + side }, new String[0])
            );
            case NONE, DLS -> FieldPermissions.DEFAULT;
        };
    }

    private static Set<BytesReference> query(IndicesPermissionAuthorizeBenchmark.DlsFls dlsFls, String side) {
        return switch (dlsFls) {
            case DLS, BOTH -> Set.of(new BytesArray("{\"term\":{\"tenant\":\"tenant-" + side + "\"}}"));
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
