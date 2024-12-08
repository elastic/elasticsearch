/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.resolve.TransportResolveClusterAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.PromoteDataStreamAction;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;
import static org.elasticsearch.xpack.core.security.support.Automatons.unionAndDeterminize;

/**
 * The name of an index related action always being with `indices:` followed by a sequence of slash-separated terms
 * that generally describes the hierarchy (from broader to more specific) of the action. For example, the
 * first level comprises `admin`, `monitor`, `data` which generally categorize an action into either an admin
 * related function, or a monitoring related function or a user-data related function. Subsequent levels further
 * narrow down the category until the meaning is specific enough.
 *
 * Note that these terms are meant to categorize what the action does, *not* how it should be invoked. This means
 * whether an action is accessible via REST API should not contribute to its naming.
 *
 * Also note that the `internal:transport/proxy/` prefix is automatically added and stripped for actions that go
 * through a CCR/CCS proxy. No action should be explicitly named like that.
 */
public final class IndexPrivilege extends Privilege {
    private static final Logger logger = LogManager.getLogger(IndexPrivilege.class);

    private static final Automaton ALL_AUTOMATON = patterns("indices:*", "internal:transport/proxy/indices:*");
    private static final Automaton READ_AUTOMATON = patterns(
        "indices:data/read/*",
        ResolveIndexAction.NAME,
        TransportResolveClusterAction.NAME
    );
    private static final Automaton READ_CROSS_CLUSTER_AUTOMATON = patterns(
        "internal:transport/proxy/indices:data/read/*",
        TransportClusterSearchShardsAction.TYPE.name(),
        TransportSearchShardsAction.TYPE.name(),
        TransportResolveClusterAction.NAME,
        "indices:data/read/esql",
        "indices:data/read/esql/compute"
    );
    private static final Automaton CREATE_AUTOMATON = patterns(
        "indices:data/write/index*",
        "indices:data/write/bulk*",
        "indices:data/write/simulate/bulk*"
    );
    private static final Automaton CREATE_DOC_AUTOMATON = patterns(
        "indices:data/write/index",
        "indices:data/write/index[*",
        "indices:data/write/index:op_type/create",
        "indices:data/write/bulk*",
        "indices:data/write/simulate/bulk*"
    );
    private static final Automaton INDEX_AUTOMATON = patterns(
        "indices:data/write/index*",
        "indices:data/write/bulk*",
        "indices:data/write/update*",
        "indices:data/write/simulate/bulk*"
    );
    private static final Automaton DELETE_AUTOMATON = patterns("indices:data/write/delete*", "indices:data/write/bulk*");
    private static final Automaton WRITE_AUTOMATON = patterns("indices:data/write/*", TransportAutoPutMappingAction.TYPE.name());
    private static final Automaton MONITOR_AUTOMATON = patterns("indices:monitor/*");
    private static final Automaton MANAGE_AUTOMATON = unionAndDeterminize(
        Arrays.asList(
            MONITOR_AUTOMATON,
            patterns("indices:admin/*", TransportFieldCapabilitiesAction.NAME + "*", GetRollupIndexCapsAction.NAME + "*")
        )
    );
    private static final Automaton CREATE_INDEX_AUTOMATON = patterns(
        TransportCreateIndexAction.TYPE.name(),
        AutoCreateAction.NAME,
        CreateDataStreamAction.NAME
    );
    private static final Automaton DELETE_INDEX_AUTOMATON = patterns(TransportDeleteIndexAction.TYPE.name(), DeleteDataStreamAction.NAME);
    private static final Automaton VIEW_METADATA_AUTOMATON = patterns(
        GetAliasesAction.NAME,
        GetIndexAction.NAME,
        GetFieldMappingsAction.NAME + "*",
        GetMappingsAction.NAME,
        TransportClusterSearchShardsAction.TYPE.name(),
        TransportSearchShardsAction.TYPE.name(),
        ValidateQueryAction.NAME + "*",
        GetSettingsAction.NAME,
        ExplainLifecycleAction.NAME,
        "indices:admin/data_stream/lifecycle/get",
        "indices:admin/data_stream/lifecycle/explain",
        GetDataStreamAction.NAME,
        ResolveIndexAction.NAME,
        TransportResolveClusterAction.NAME,
        TransportFieldCapabilitiesAction.NAME + "*",
        GetRollupIndexCapsAction.NAME + "*",
        GetCheckpointAction.NAME + "*", // transform internal action
        "indices:monitor/get/metering/stats", // serverless only
        "indices:admin/get/metering/stats" // serverless only
    );
    private static final Automaton MANAGE_FOLLOW_INDEX_AUTOMATON = patterns(
        PutFollowAction.NAME,
        UnfollowAction.NAME,
        TransportCloseIndexAction.NAME + "*",
        PromoteDataStreamAction.NAME,
        RolloverAction.NAME
    );
    private static final Automaton MANAGE_LEADER_INDEX_AUTOMATON = patterns(ForgetFollowerAction.NAME + "*");
    private static final Automaton MANAGE_ILM_AUTOMATON = patterns("indices:admin/ilm/*");
    private static final Automaton MANAGE_DATA_STREAM_LIFECYCLE_AUTOMATON = patterns("indices:admin/data_stream/lifecycle/*");
    private static final Automaton MAINTENANCE_AUTOMATON = patterns(
        "indices:admin/refresh*",
        "indices:admin/flush*",
        "indices:admin/synced_flush",
        "indices:admin/forcemerge*"
    );
    private static final Automaton AUTO_CONFIGURE_AUTOMATON = patterns(TransportAutoPutMappingAction.TYPE.name(), AutoCreateAction.NAME);

    private static final Automaton CROSS_CLUSTER_REPLICATION_AUTOMATON = patterns(
        "indices:data/read/xpack/ccr/shard_changes*",
        IndicesStatsAction.NAME + "*",
        RetentionLeaseActions.ADD.name() + "*",
        RetentionLeaseActions.REMOVE.name() + "*",
        RetentionLeaseActions.RENEW.name() + "*"
    );
    private static final Automaton CROSS_CLUSTER_REPLICATION_INTERNAL_AUTOMATON = patterns(
        "indices:internal/admin/ccr/restore/session/clear*",
        "indices:internal/admin/ccr/restore/file_chunk/get*",
        "indices:internal/admin/ccr/restore/session/put*",
        "internal:transport/proxy/indices:internal/admin/ccr/restore/session/clear*",
        "internal:transport/proxy/indices:internal/admin/ccr/restore/file_chunk/get*"
    );

    public static final IndexPrivilege NONE = new IndexPrivilege("none", Automatons.EMPTY);
    public static final IndexPrivilege ALL = new IndexPrivilege("all", ALL_AUTOMATON);
    public static final IndexPrivilege READ = new IndexPrivilege("read", READ_AUTOMATON);
    public static final IndexPrivilege READ_CROSS_CLUSTER = new IndexPrivilege("read_cross_cluster", READ_CROSS_CLUSTER_AUTOMATON);
    public static final IndexPrivilege CREATE = new IndexPrivilege("create", CREATE_AUTOMATON);
    public static final IndexPrivilege INDEX = new IndexPrivilege("index", INDEX_AUTOMATON);
    public static final IndexPrivilege DELETE = new IndexPrivilege("delete", DELETE_AUTOMATON);
    public static final IndexPrivilege WRITE = new IndexPrivilege("write", WRITE_AUTOMATON);
    public static final IndexPrivilege CREATE_DOC = new IndexPrivilege("create_doc", CREATE_DOC_AUTOMATON);
    public static final IndexPrivilege MONITOR = new IndexPrivilege("monitor", MONITOR_AUTOMATON);
    public static final IndexPrivilege MANAGE = new IndexPrivilege("manage", MANAGE_AUTOMATON);
    public static final IndexPrivilege DELETE_INDEX = new IndexPrivilege("delete_index", DELETE_INDEX_AUTOMATON);
    public static final IndexPrivilege CREATE_INDEX = new IndexPrivilege("create_index", CREATE_INDEX_AUTOMATON);
    public static final IndexPrivilege VIEW_METADATA = new IndexPrivilege("view_index_metadata", VIEW_METADATA_AUTOMATON);
    public static final IndexPrivilege MANAGE_FOLLOW_INDEX = new IndexPrivilege("manage_follow_index", MANAGE_FOLLOW_INDEX_AUTOMATON);
    public static final IndexPrivilege MANAGE_LEADER_INDEX = new IndexPrivilege("manage_leader_index", MANAGE_LEADER_INDEX_AUTOMATON);
    public static final IndexPrivilege MANAGE_ILM = new IndexPrivilege("manage_ilm", MANAGE_ILM_AUTOMATON);
    public static final IndexPrivilege MANAGE_DATA_STREAM_LIFECYCLE = new IndexPrivilege(
        "manage_data_stream_lifecycle",
        MANAGE_DATA_STREAM_LIFECYCLE_AUTOMATON
    );
    public static final IndexPrivilege MAINTENANCE = new IndexPrivilege("maintenance", MAINTENANCE_AUTOMATON);
    public static final IndexPrivilege AUTO_CONFIGURE = new IndexPrivilege("auto_configure", AUTO_CONFIGURE_AUTOMATON);
    public static final IndexPrivilege CROSS_CLUSTER_REPLICATION = new IndexPrivilege(
        "cross_cluster_replication",
        CROSS_CLUSTER_REPLICATION_AUTOMATON
    );
    public static final IndexPrivilege CROSS_CLUSTER_REPLICATION_INTERNAL = new IndexPrivilege(
        "cross_cluster_replication_internal",
        CROSS_CLUSTER_REPLICATION_INTERNAL_AUTOMATON
    );

    /**
     * If you are adding a new named index privilege, also add it to the
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-privileges.html#privileges-list-indices">docs</a>.
     */
    @SuppressWarnings("unchecked")
    private static final Map<String, IndexPrivilege> VALUES = sortByAccessLevel(
        Stream.of(
            entry("none", NONE),
            entry("all", ALL),
            entry("manage", MANAGE),
            entry("create_index", CREATE_INDEX),
            entry("monitor", MONITOR),
            entry("read", READ),
            entry("index", INDEX),
            entry("delete", DELETE),
            entry("write", WRITE),
            entry("create", CREATE),
            entry("create_doc", CREATE_DOC),
            entry("delete_index", DELETE_INDEX),
            entry("view_index_metadata", VIEW_METADATA),
            entry("read_cross_cluster", READ_CROSS_CLUSTER),
            entry("manage_follow_index", MANAGE_FOLLOW_INDEX),
            entry("manage_leader_index", MANAGE_LEADER_INDEX),
            entry("manage_ilm", MANAGE_ILM),
            entry("manage_data_stream_lifecycle", MANAGE_DATA_STREAM_LIFECYCLE),
            entry("maintenance", MAINTENANCE),
            entry("auto_configure", AUTO_CONFIGURE),
            entry("cross_cluster_replication", CROSS_CLUSTER_REPLICATION),
            entry("cross_cluster_replication_internal", CROSS_CLUSTER_REPLICATION_INTERNAL)
        ).filter(Objects::nonNull).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    public static final Predicate<String> ACTION_MATCHER = ALL.predicate();
    public static final Predicate<String> CREATE_INDEX_MATCHER = CREATE_INDEX.predicate();

    private static final ConcurrentHashMap<Set<String>, IndexPrivilege> CACHE = new ConcurrentHashMap<>();

    private IndexPrivilege(String name, Automaton automaton) {
        super(Collections.singleton(name), automaton);
    }

    private IndexPrivilege(Set<String> name, Automaton automaton) {
        super(name, automaton);
    }

    public static IndexPrivilege get(Set<String> name) {
        return CACHE.computeIfAbsent(name, (theName) -> {
            if (theName.isEmpty()) {
                return NONE;
            } else {
                return resolve(theName);
            }
        });
    }

    @Nullable
    public static IndexPrivilege getNamedOrNull(String name) {
        return VALUES.get(name.toLowerCase(Locale.ROOT));
    }

    private static IndexPrivilege resolve(Set<String> name) {
        final int size = name.size();
        if (size == 0) {
            throw new IllegalArgumentException("empty set should not be used");
        }

        Set<String> actions = new HashSet<>();
        Set<Automaton> automata = new HashSet<>();
        for (String part : name) {
            part = part.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(part)) {
                actions.add(actionToPattern(part));
            } else {
                IndexPrivilege indexPrivilege = part == null ? null : VALUES.get(part);
                if (indexPrivilege != null && size == 1) {
                    return indexPrivilege;
                } else if (indexPrivilege != null) {
                    automata.add(indexPrivilege.automaton);
                } else {
                    String errorMessage = "unknown index privilege ["
                        + part
                        + "]. a privilege must be either "
                        + "one of the predefined fixed indices privileges ["
                        + Strings.collectionToCommaDelimitedString(VALUES.entrySet())
                        + "] or a pattern over one of the available index"
                        + " actions";
                    logger.debug(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }

        if (actions.isEmpty() == false) {
            automata.add(patterns(actions));
        }
        return new IndexPrivilege(name, unionAndDeterminize(automata));
    }

    static Map<String, IndexPrivilege> values() {
        return VALUES;
    }

    public static Set<String> names() {
        return Collections.unmodifiableSet(VALUES.keySet());
    }

    /**
     * Returns the names of privileges that grant the specified action.
     * @return A collection of names, ordered (to the extent possible) from least privileged (e.g. {@link #CREATE_DOC})
     * to most privileged (e.g. {@link #ALL})
     * @see Privilege#sortByAccessLevel
     */
    public static Collection<String> findPrivilegesThatGrant(String action) {
        return VALUES.entrySet().stream().filter(e -> e.getValue().predicate.test(action)).map(e -> e.getKey()).toList();
    }
}
