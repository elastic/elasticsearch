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
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;
import static org.elasticsearch.xpack.core.security.support.Automatons.unionAndMinimize;

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
 *
 * Each named privilege is associated with an {@link IndexComponentSelector} it grants access to.
 */
public final class IndexPrivilege extends Privilege {
    private static final Logger logger = LogManager.getLogger(IndexPrivilege.class);

    private static final Automaton ALL_AUTOMATON = patterns("indices:*", "internal:transport/proxy/indices:*");
    private static final Automaton READ_AUTOMATON = patterns(
        "indices:data/read/*",
        ResolveIndexAction.NAME,
        TransportResolveClusterAction.NAME
    );
    private static final Automaton READ_FAILURE_STORE_AUTOMATON = patterns("indices:data/read/*", ResolveIndexAction.NAME);
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
    private static final Automaton MANAGE_AUTOMATON = unionAndMinimize(
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
    public static final IndexPrivilege ALL = new IndexPrivilege("all", ALL_AUTOMATON, IndexComponentSelectorPredicate.ALL);
    public static final IndexPrivilege READ = new IndexPrivilege("read", READ_AUTOMATON);
    public static final IndexPrivilege READ_CROSS_CLUSTER = new IndexPrivilege("read_cross_cluster", READ_CROSS_CLUSTER_AUTOMATON);
    public static final IndexPrivilege CREATE = new IndexPrivilege("create", CREATE_AUTOMATON);
    public static final IndexPrivilege INDEX = new IndexPrivilege("index", INDEX_AUTOMATON);
    public static final IndexPrivilege DELETE = new IndexPrivilege("delete", DELETE_AUTOMATON);
    public static final IndexPrivilege WRITE = new IndexPrivilege("write", WRITE_AUTOMATON);
    public static final IndexPrivilege CREATE_DOC = new IndexPrivilege("create_doc", CREATE_DOC_AUTOMATON);
    public static final IndexPrivilege MONITOR = new IndexPrivilege("monitor", MONITOR_AUTOMATON);
    public static final IndexPrivilege MANAGE = new IndexPrivilege(
        "manage",
        MANAGE_AUTOMATON,
        IndexComponentSelectorPredicate.DATA_AND_FAILURES
    );
    public static final IndexPrivilege DELETE_INDEX = new IndexPrivilege("delete_index", DELETE_INDEX_AUTOMATON);
    public static final IndexPrivilege CREATE_INDEX = new IndexPrivilege("create_index", CREATE_INDEX_AUTOMATON);
    public static final IndexPrivilege VIEW_METADATA = new IndexPrivilege("view_index_metadata", VIEW_METADATA_AUTOMATON);
    public static final IndexPrivilege MANAGE_FOLLOW_INDEX = new IndexPrivilege("manage_follow_index", MANAGE_FOLLOW_INDEX_AUTOMATON);
    public static final IndexPrivilege MANAGE_LEADER_INDEX = new IndexPrivilege("manage_leader_index", MANAGE_LEADER_INDEX_AUTOMATON);
    public static final IndexPrivilege MANAGE_ILM = new IndexPrivilege("manage_ilm", MANAGE_ILM_AUTOMATON);
    public static final IndexPrivilege MANAGE_DATA_STREAM_LIFECYCLE = new IndexPrivilege(
        "manage_data_stream_lifecycle",
        MANAGE_DATA_STREAM_LIFECYCLE_AUTOMATON,
        IndexComponentSelectorPredicate.DATA_AND_FAILURES
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

    public static final IndexPrivilege READ_FAILURE_STORE = new IndexPrivilege(
        "read_failure_store",
        READ_FAILURE_STORE_AUTOMATON,
        IndexComponentSelectorPredicate.FAILURES
    );
    public static final IndexPrivilege MANAGE_FAILURE_STORE = new IndexPrivilege(
        "manage_failure_store",
        MANAGE_AUTOMATON,
        IndexComponentSelectorPredicate.FAILURES
    );

    /**
     * If you are adding a new named index privilege, also add it to the
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-privileges.html#privileges-list-indices">docs</a>.
     */
    private static final Map<String, IndexPrivilege> VALUES = combineSortedInOrder(
        sortByAccessLevel(
            Stream.of(entry("read_failure_store", READ_FAILURE_STORE), entry("manage_failure_store", MANAGE_FAILURE_STORE))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
        ),
        sortByAccessLevel(
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
            ).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue))
        )
    );

    private static Map<String, IndexPrivilege> combineSortedInOrder(
        SortedMap<String, IndexPrivilege> first,
        SortedMap<String, IndexPrivilege> second
    ) {
        if (first.isEmpty()) {
            return second;
        }
        if (second.isEmpty()) {
            return first;
        }
        final Map<String, IndexPrivilege> combined = new LinkedHashMap<>(first);
        combined.putAll(second);
        return Collections.unmodifiableMap(combined);
    }

    public static final Predicate<String> ACTION_MATCHER = ALL.predicate();
    public static final Predicate<String> CREATE_INDEX_MATCHER = CREATE_INDEX.predicate();

    private static final ConcurrentHashMap<Set<String>, Set<IndexPrivilege>> CACHE = new ConcurrentHashMap<>();

    private final IndexComponentSelectorPredicate selectorPredicate;

    private IndexPrivilege(String name, Automaton automaton) {
        this(Collections.singleton(name), automaton);
    }

    private IndexPrivilege(String name, Automaton automaton, IndexComponentSelectorPredicate selectorPredicate) {
        this(Collections.singleton(name), automaton, selectorPredicate);
    }

    private IndexPrivilege(Set<String> name, Automaton automaton) {
        this(name, automaton, IndexComponentSelectorPredicate.DATA);
    }

    private IndexPrivilege(Set<String> name, Automaton automaton, IndexComponentSelectorPredicate selectorPredicate) {
        super(name, automaton);
        this.selectorPredicate = selectorPredicate;
    }

    /**
     * Returns a {@link IndexPrivilege} that corresponds to the given raw action pattern or privilege name.
     */
    public static IndexPrivilege get(String actionOrPrivilege) {
        final Set<IndexPrivilege> privilegeSingleton = resolveBySelectorAccess(Set.of(actionOrPrivilege));
        if (privilegeSingleton.size() != 1) {
            throw new IllegalArgumentException(
                "index privilege name or action "
                    + actionOrPrivilege
                    + " must map to exactly one privilege but mapped to "
                    + privilegeSingleton
            );
        }
        return privilegeSingleton.iterator().next();
    }

    /**
     * Returns a set {@link IndexPrivilege} that captures the access granted by the privileges and actions specified in the input name set.
     * This method returns a set of index privileges, instead of a single index privilege to capture that different index privileges grant
     * access to different {@link IndexComponentSelector}s. Most privileges grant access to the
     * (implicit) {@link IndexComponentSelector#DATA} selector. The {@link IndexPrivilege#READ_FAILURE_STORE} grants access to
     * {@link IndexComponentSelector#FAILURES}.
     * The implementation for authorization for access by selector requires that index privileges are (generally) not combined across
     * selector boundaries since their underlying automata would be combined, granting more access than is valid.
     * This method conceptually splits the input names into ones that correspond to different selector access, and return an index privilege
     * for each partition.
     * For instance, `resolveBySelectorAccess(Set.of("view_index_metadata", "write", "read_failure_store"))` will return two index
     * privileges one covering `view_index_metadata` and `write` for a {@link IndexComponentSelectorPredicate#DATA}, the other covering
     * `read_failure_store` for a {@link IndexComponentSelectorPredicate#FAILURES} selector.
     * A notable exception is the {@link IndexPrivilege#ALL} privilege. If this privilege is included in the input name set, this method
     * returns a single index privilege that grants access to all selectors.
     * All raw actions are treated as granting access to the {@link IndexComponentSelector#DATA} selector.
     */
    public static Set<IndexPrivilege> resolveBySelectorAccess(Set<String> names) {
        return CACHE.computeIfAbsent(names, (theName) -> {
            if (theName.isEmpty()) {
                return Set.of(NONE);
            } else {
                return resolve(theName);
            }
        });
    }

    @Nullable
    public static IndexPrivilege getNamedOrNull(String name) {
        return VALUES.get(name.toLowerCase(Locale.ROOT));
    }

    private static Set<IndexPrivilege> resolve(Set<String> name) {
        final int size = name.size();
        if (size == 0) {
            throw new IllegalArgumentException("empty set should not be used");
        }

        final Set<String> actions = new HashSet<>();
        final Set<IndexPrivilege> allSelectorAccessPrivileges = new HashSet<>();
        final Set<IndexPrivilege> dataSelectorAccessPrivileges = new HashSet<>();
        final Set<IndexPrivilege> failuresSelectorAccessPrivileges = new HashSet<>();
        final Set<IndexPrivilege> dataAndFailuresSelectorAccessPrivileges = new HashSet<>();

        boolean containsAllAccessPrivilege = name.stream().anyMatch(n -> getNamedOrNull(n) == ALL);
        for (String part : name) {
            part = part.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(part)) {
                actions.add(part);
            } else {
                IndexPrivilege indexPrivilege = part == null ? null : VALUES.get(part);
                if (indexPrivilege != null && size == 1) {
                    return Set.of(indexPrivilege);
                } else if (indexPrivilege != null) {
                    // if we have an all access privilege, we don't need to partition anymore since it grants access to all selectors and
                    // any other name in the group has its selector-access superseded.
                    if (containsAllAccessPrivilege) {
                        allSelectorAccessPrivileges.add(indexPrivilege);
                    } else if (indexPrivilege.selectorPredicate == IndexComponentSelectorPredicate.DATA) {
                        dataSelectorAccessPrivileges.add(indexPrivilege);
                    } else if (indexPrivilege.selectorPredicate == IndexComponentSelectorPredicate.FAILURES) {
                        failuresSelectorAccessPrivileges.add(indexPrivilege);
                    } else if (indexPrivilege.selectorPredicate == IndexComponentSelectorPredicate.DATA_AND_FAILURES) {
                        dataAndFailuresSelectorAccessPrivileges.add(indexPrivilege);
                    } else {
                        String errorMessage = "unexpected selector [" + indexPrivilege.selectorPredicate + "]";
                        assert false : errorMessage;
                        throw new IllegalStateException(errorMessage);
                    }
                } else {
                    String errorMessage = "unknown index privilege ["
                        + part
                        + "]. a privilege must be either "
                        + "one of the predefined fixed indices privileges ["
                        + Strings.collectionToCommaDelimitedString(names().stream().sorted().collect(Collectors.toList()))
                        + "] or a pattern over one of the available index"
                        + " actions";
                    logger.debug(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }

        final Set<IndexPrivilege> combined = combineIndexPrivileges(
            allSelectorAccessPrivileges,
            dataSelectorAccessPrivileges,
            failuresSelectorAccessPrivileges,
            dataAndFailuresSelectorAccessPrivileges,
            actions
        );
        assertNamesMatch(name, combined);
        return Collections.unmodifiableSet(combined);
    }

    private static Set<IndexPrivilege> combineIndexPrivileges(
        Set<IndexPrivilege> allSelectorAccessPrivileges,
        Set<IndexPrivilege> dataSelectorAccessPrivileges,
        Set<IndexPrivilege> failuresSelectorAccessPrivileges,
        Set<IndexPrivilege> dataAndFailuresSelectorAccessPrivileges,
        Set<String> actions
    ) {
        assert false == allSelectorAccessPrivileges.isEmpty()
            || false == dataSelectorAccessPrivileges.isEmpty()
            || false == failuresSelectorAccessPrivileges.isEmpty()
            || false == dataAndFailuresSelectorAccessPrivileges.isEmpty()
            || false == actions.isEmpty() : "at least one of the privilege sets or actions must be non-empty";

        if (false == allSelectorAccessPrivileges.isEmpty()) {
            assert failuresSelectorAccessPrivileges.isEmpty()
                && dataSelectorAccessPrivileges.isEmpty()
                && dataAndFailuresSelectorAccessPrivileges.isEmpty() : "data and failure access must be empty when all access is present";
            return Set.of(union(allSelectorAccessPrivileges, actions, IndexComponentSelectorPredicate.ALL));
        }

        // linked hash set to preserve order across selectors
        final Set<IndexPrivilege> combined = Sets.newLinkedHashSetWithExpectedSize(
            dataAndFailuresSelectorAccessPrivileges.size() + failuresSelectorAccessPrivileges.size() + dataSelectorAccessPrivileges.size()
                + actions.size()
        );
        if (false == dataSelectorAccessPrivileges.isEmpty() || false == actions.isEmpty()) {
            combined.add(union(dataSelectorAccessPrivileges, actions, IndexComponentSelectorPredicate.DATA));
        }
        if (false == dataAndFailuresSelectorAccessPrivileges.isEmpty()) {
            combined.add(union(dataAndFailuresSelectorAccessPrivileges, Set.of(), IndexComponentSelectorPredicate.DATA_AND_FAILURES));
        }
        if (false == failuresSelectorAccessPrivileges.isEmpty()) {
            combined.add(union(failuresSelectorAccessPrivileges, Set.of(), IndexComponentSelectorPredicate.FAILURES));
        }
        return combined;
    }

    private static void assertNamesMatch(Set<String> names, Set<IndexPrivilege> privileges) {
        assert names.stream()
            .map(n -> n.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet())
            .equals(privileges.stream().map(Privilege::name).flatMap(Set::stream).collect(Collectors.toSet()))
            : "mismatch between names [" + names + "] and names on split privileges [" + privileges + "]";
    }

    private static IndexPrivilege union(
        Collection<IndexPrivilege> privileges,
        Collection<String> actions,
        IndexComponentSelectorPredicate selectorPredicate
    ) {
        final Set<Automaton> automata = Sets.newHashSetWithExpectedSize(privileges.size() + actions.size());
        final Set<String> names = Sets.newHashSetWithExpectedSize(privileges.size() + actions.size());
        for (IndexPrivilege privilege : privileges) {
            names.addAll(privilege.name());
            automata.add(privilege.automaton);
        }

        if (false == actions.isEmpty()) {
            names.addAll(actions);
            automata.add(patterns(actions.stream().map(Privilege::actionToPattern).toList()));
        }
        return new IndexPrivilege(names, unionAndMinimize(automata), selectorPredicate);
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
        return findPrivilegesThatGrant(action, p -> p.getSelectorPredicate().test(IndexComponentSelector.DATA));
    }

    public static Collection<String> findPrivilegesThatGrant(String action, Predicate<IndexPrivilege> preCondition) {
        return VALUES.entrySet()
            .stream()
            .filter(e -> preCondition.test(e.getValue()))
            .filter(e -> e.getValue().predicate.test(action))
            .map(Map.Entry::getKey)
            .toList();
    }

    public IndexComponentSelectorPredicate getSelectorPredicate() {
        return selectorPredicate;
    }
}
