/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.StartILMAction;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Translates cluster privilege names into concrete implementations
 */
public class ClusterPrivilegeResolver {
    private static final Logger logger = LogManager.getLogger(ClusterPrivilegeResolver.class);

    // shared automatons
    private static final Set<String> ALL_SECURITY_PATTERN = Set.of("cluster:admin/xpack/security/*");
    private static final Set<String> MANAGE_SAML_PATTERN = Set.of(
        "cluster:admin/xpack/security/saml/*",
        InvalidateTokenAction.NAME,
        RefreshTokenAction.NAME,
        SamlSpMetadataAction.NAME
    );
    private static final Set<String> MANAGE_OIDC_PATTERN = Set.of("cluster:admin/xpack/security/oidc/*");
    private static final Set<String> MANAGE_TOKEN_PATTERN = Set.of("cluster:admin/xpack/security/token/*");
    private static final Set<String> MANAGE_API_KEY_PATTERN = Set.of("cluster:admin/xpack/security/api_key/*");
    private static final Set<String> MANAGE_BEHAVIORAL_ANALYTICS_PATTERN = Set.of("cluster:admin/xpack/application/analytics/*");
    private static final Set<String> POST_BEHAVIORAL_ANALYTICS_EVENT_PATTERN = Set.of(
        "cluster:admin/xpack/application/analytics/post_event"
    );
    private static final Set<String> MANAGE_SERVICE_ACCOUNT_PATTERN = Set.of("cluster:admin/xpack/security/service_account/*");
    private static final Set<String> MANAGE_USER_PROFILE_PATTERN = Set.of("cluster:admin/xpack/security/profile/*");
    private static final Set<String> GRANT_API_KEY_PATTERN = Set.of(GrantApiKeyAction.NAME + "*");
    private static final Set<String> MONITOR_PATTERN = Set.of(
        "cluster:monitor/*",
        GetIndexTemplatesAction.NAME,
        GetComponentTemplateAction.NAME,
        GetComposableIndexTemplateAction.NAME
    );
    private static final Set<String> MONITOR_ML_PATTERN = Set.of("cluster:monitor/xpack/ml/*");
    private static final Set<String> MONITOR_TEXT_STRUCTURE_PATTERN = Set.of("cluster:monitor/text_structure/*");
    private static final Set<String> MONITOR_TRANSFORM_PATTERN = Set.of("cluster:monitor/data_frame/*", "cluster:monitor/transform/*");
    private static final Set<String> MONITOR_WATCHER_PATTERN = Set.of("cluster:monitor/xpack/watcher/*");
    private static final Set<String> MONITOR_ROLLUP_PATTERN = Set.of("cluster:monitor/xpack/rollup/*");
    private static final Set<String> ALL_CLUSTER_PATTERN = Set.of(
        "cluster:*",
        "indices:admin/template/*",
        "indices:admin/index_template/*"
    );
    private static final Predicate<String> ACTION_MATCHER = Automatons.predicate(ALL_CLUSTER_PATTERN);
    private static final Set<String> MANAGE_ML_PATTERN = Set.of("cluster:admin/xpack/ml/*", "cluster:monitor/xpack/ml/*");
    private static final Set<String> MANAGE_TRANSFORM_PATTERN = Set.of(
        "cluster:admin/data_frame/*",
        "cluster:monitor/data_frame/*",
        "cluster:monitor/transform/*",
        "cluster:admin/transform/*"
    );
    private static final Set<String> MANAGE_WATCHER_PATTERN = Set.of("cluster:admin/xpack/watcher/*", "cluster:monitor/xpack/watcher/*");
    private static final Set<String> TRANSPORT_CLIENT_PATTERN = Set.of("cluster:monitor/nodes/liveness", "cluster:monitor/state");
    private static final Set<String> MANAGE_IDX_TEMPLATE_PATTERN = Set.of(
        "indices:admin/template/*",
        "indices:admin/index_template/*",
        "cluster:admin/component_template/*"
    );
    private static final Set<String> MANAGE_INGEST_PIPELINE_PATTERN = Set.of("cluster:admin/ingest/pipeline/*");
    private static final Set<String> READ_PIPELINE_PATTERN = Set.of(GetPipelineAction.NAME, SimulatePipelineAction.NAME);
    private static final Set<String> MANAGE_ROLLUP_PATTERN = Set.of("cluster:admin/xpack/rollup/*", "cluster:monitor/xpack/rollup/*");
    private static final Set<String> MANAGE_CCR_PATTERN = Set.of(
        "cluster:admin/xpack/ccr/*",
        ClusterStateAction.NAME,
        HasPrivilegesAction.NAME
    );
    private static final Set<String> CREATE_SNAPSHOT_PATTERN = Set.of(
        CreateSnapshotAction.NAME,
        SnapshotsStatusAction.NAME + "*",
        GetSnapshotsAction.NAME,
        SnapshotsStatusAction.NAME,
        GetRepositoriesAction.NAME
    );
    private static final Set<String> MONITOR_SNAPSHOT_PATTERN = Set.of(
        SnapshotsStatusAction.NAME + "*",
        GetSnapshotsAction.NAME,
        SnapshotsStatusAction.NAME,
        GetRepositoriesAction.NAME
    );
    private static final Set<String> READ_CCR_PATTERN = Set.of(ClusterStateAction.NAME, HasPrivilegesAction.NAME);
    private static final Set<String> MANAGE_ILM_PATTERN = Set.of("cluster:admin/ilm/*");
    private static final Set<String> READ_ILM_PATTERN = Set.of(GetLifecycleAction.NAME, GetStatusAction.NAME);
    private static final Set<String> MANAGE_SLM_PATTERN = Set.of(
        "cluster:admin/slm/*",
        StartILMAction.NAME,
        StopILMAction.NAME,
        GetStatusAction.NAME
    );
    private static final Set<String> READ_SLM_PATTERN = Set.of(GetSnapshotLifecycleAction.NAME, GetStatusAction.NAME);

    private static final Set<String> MANAGE_SEARCH_APPLICATION_PATTERN = Set.of("cluster:admin/xpack/application/search_application/*");

    private static final Set<String> CROSS_CLUSTER_SEARCH_PATTERN = Set.of(
        RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME,
        RemoteClusterNodesAction.NAME,
        XPackInfoAction.NAME
    );
    private static final Set<String> CROSS_CLUSTER_REPLICATION_PATTERN = Set.of(
        RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME,
        RemoteClusterNodesAction.NAME,
        XPackInfoAction.NAME,
        ClusterStateAction.NAME
    );
    private static final Set<String> MANAGE_ENRICH_AUTOMATON = Set.of("cluster:admin/xpack/enrich/*");

    public static final NamedClusterPrivilege NONE = new ActionClusterPrivilege("none", Set.of(), Set.of());
    public static final NamedClusterPrivilege ALL = new ActionClusterPrivilege("all", ALL_CLUSTER_PATTERN);
    public static final NamedClusterPrivilege MONITOR = new ActionClusterPrivilege("monitor", MONITOR_PATTERN);
    public static final NamedClusterPrivilege MONITOR_ML = new ActionClusterPrivilege("monitor_ml", MONITOR_ML_PATTERN);
    public static final NamedClusterPrivilege MONITOR_TRANSFORM_DEPRECATED = new ActionClusterPrivilege(
        "monitor_data_frame_transforms",
        MONITOR_TRANSFORM_PATTERN
    );
    public static final NamedClusterPrivilege MONITOR_TEXT_STRUCTURE = new ActionClusterPrivilege(
        "monitor_text_structure",
        MONITOR_TEXT_STRUCTURE_PATTERN
    );
    public static final NamedClusterPrivilege MONITOR_TRANSFORM = new ActionClusterPrivilege(
        "monitor_transform",
        MONITOR_TRANSFORM_PATTERN
    );
    public static final NamedClusterPrivilege MONITOR_WATCHER = new ActionClusterPrivilege("monitor_watcher", MONITOR_WATCHER_PATTERN);
    public static final NamedClusterPrivilege MONITOR_ROLLUP = new ActionClusterPrivilege("monitor_rollup", MONITOR_ROLLUP_PATTERN);
    public static final NamedClusterPrivilege MANAGE = new ActionClusterPrivilege("manage", ALL_CLUSTER_PATTERN, ALL_SECURITY_PATTERN);
    public static final NamedClusterPrivilege MANAGE_ML = new ActionClusterPrivilege("manage_ml", MANAGE_ML_PATTERN);
    public static final NamedClusterPrivilege MANAGE_TRANSFORM_DEPRECATED = new ActionClusterPrivilege(
        "manage_data_frame_transforms",
        MANAGE_TRANSFORM_PATTERN
    );
    public static final NamedClusterPrivilege MANAGE_TRANSFORM = new ActionClusterPrivilege("manage_transform", MANAGE_TRANSFORM_PATTERN);
    public static final NamedClusterPrivilege MANAGE_TOKEN = new ActionClusterPrivilege("manage_token", MANAGE_TOKEN_PATTERN);
    public static final NamedClusterPrivilege MANAGE_WATCHER = new ActionClusterPrivilege("manage_watcher", MANAGE_WATCHER_PATTERN);
    public static final NamedClusterPrivilege MANAGE_ROLLUP = new ActionClusterPrivilege("manage_rollup", MANAGE_ROLLUP_PATTERN);
    public static final NamedClusterPrivilege MANAGE_IDX_TEMPLATES = new ActionClusterPrivilege(
        "manage_index_templates",
        MANAGE_IDX_TEMPLATE_PATTERN
    );
    public static final NamedClusterPrivilege MANAGE_INGEST_PIPELINES = new ActionClusterPrivilege(
        "manage_ingest_pipelines",
        MANAGE_INGEST_PIPELINE_PATTERN
    );
    public static final NamedClusterPrivilege READ_PIPELINE = new ActionClusterPrivilege("read_pipeline", READ_PIPELINE_PATTERN);
    public static final NamedClusterPrivilege TRANSPORT_CLIENT = new ActionClusterPrivilege("transport_client", TRANSPORT_CLIENT_PATTERN);
    public static final NamedClusterPrivilege MANAGE_SECURITY = new ActionClusterPrivilege(
        "manage_security",
        ALL_SECURITY_PATTERN,
        Set.of(DelegatePkiAuthenticationAction.NAME)
    );
    public static final NamedClusterPrivilege READ_SECURITY = new ActionClusterPrivilege(
        "read_security",
        Set.of(
            GetApiKeyAction.NAME,
            QueryApiKeyAction.NAME,
            GetBuiltinPrivilegesAction.NAME,
            GetPrivilegesAction.NAME,
            GetProfilesAction.NAME,
            ProfileHasPrivilegesAction.NAME,
            SuggestProfilesAction.NAME,
            GetRolesAction.NAME,
            GetRoleMappingsAction.NAME,
            GetServiceAccountAction.NAME,
            GetServiceAccountCredentialsAction.NAME + "*",
            GetUsersAction.NAME,
            GetUserPrivilegesAction.NAME, // normally authorized under the "same-user" authz check, but added here for uniformity
            HasPrivilegesAction.NAME
        )
    );
    public static final NamedClusterPrivilege MANAGE_SAML = new ActionClusterPrivilege("manage_saml", MANAGE_SAML_PATTERN);
    public static final NamedClusterPrivilege MANAGE_OIDC = new ActionClusterPrivilege("manage_oidc", MANAGE_OIDC_PATTERN);
    public static final NamedClusterPrivilege MANAGE_API_KEY = new ActionClusterPrivilege("manage_api_key", MANAGE_API_KEY_PATTERN);
    public static final NamedClusterPrivilege MANAGE_SERVICE_ACCOUNT = new ActionClusterPrivilege(
        "manage_service_account",
        MANAGE_SERVICE_ACCOUNT_PATTERN
    );
    public static final NamedClusterPrivilege MANAGE_USER_PROFILE = new ActionClusterPrivilege(
        "manage_user_profile",
        MANAGE_USER_PROFILE_PATTERN
    );
    public static final NamedClusterPrivilege GRANT_API_KEY = new ActionClusterPrivilege("grant_api_key", GRANT_API_KEY_PATTERN);
    public static final NamedClusterPrivilege MANAGE_PIPELINE = new ActionClusterPrivilege(
        "manage_pipeline",
        Set.of("cluster:admin" + "/ingest/pipeline/*")
    );
    public static final NamedClusterPrivilege MANAGE_AUTOSCALING = new ActionClusterPrivilege(
        "manage_autoscaling",
        Set.of("cluster:admin/autoscaling/*")
    );
    public static final NamedClusterPrivilege MANAGE_CCR = new ActionClusterPrivilege("manage_ccr", MANAGE_CCR_PATTERN);
    public static final NamedClusterPrivilege READ_CCR = new ActionClusterPrivilege("read_ccr", READ_CCR_PATTERN);
    public static final NamedClusterPrivilege CREATE_SNAPSHOT = new ActionClusterPrivilege("create_snapshot", CREATE_SNAPSHOT_PATTERN);
    public static final NamedClusterPrivilege MONITOR_SNAPSHOT = new ActionClusterPrivilege("monitor_snapshot", MONITOR_SNAPSHOT_PATTERN);
    public static final NamedClusterPrivilege MANAGE_ILM = new ActionClusterPrivilege("manage_ilm", MANAGE_ILM_PATTERN);
    public static final NamedClusterPrivilege READ_ILM = new ActionClusterPrivilege("read_ilm", READ_ILM_PATTERN);
    public static final NamedClusterPrivilege MANAGE_SLM = new ActionClusterPrivilege("manage_slm", MANAGE_SLM_PATTERN);
    public static final NamedClusterPrivilege READ_SLM = new ActionClusterPrivilege("read_slm", READ_SLM_PATTERN);
    public static final NamedClusterPrivilege DELEGATE_PKI = new ActionClusterPrivilege(
        "delegate_pki",
        Set.of(DelegatePkiAuthenticationAction.NAME, InvalidateTokenAction.NAME)
    );

    public static final NamedClusterPrivilege MANAGE_OWN_API_KEY = ManageOwnApiKeyClusterPrivilege.INSTANCE;
    public static final NamedClusterPrivilege MANAGE_ENRICH = new ActionClusterPrivilege("manage_enrich", MANAGE_ENRICH_AUTOMATON);

    public static final NamedClusterPrivilege MANAGE_LOGSTASH_PIPELINES = new ActionClusterPrivilege(
        "manage_logstash_pipelines",
        Set.of("cluster:admin/logstash/pipeline/*")
    );

    public static final NamedClusterPrivilege CANCEL_TASK = new ActionClusterPrivilege("cancel_task", Set.of(CancelTasksAction.NAME + "*"));

    public static final NamedClusterPrivilege MANAGE_SEARCH_APPLICATION = new ActionClusterPrivilege(
        "manage_search_application",
        MANAGE_SEARCH_APPLICATION_PATTERN
    );

    public static final NamedClusterPrivilege MANAGE_BEHAVIORAL_ANALYTICS = new ActionClusterPrivilege(
        "manage_behavioral_analytics",
        MANAGE_BEHAVIORAL_ANALYTICS_PATTERN
    );

    public static final NamedClusterPrivilege POST_BEHAVIORAL_ANALYTICS_EVENT = new ActionClusterPrivilege(
        "post_behavioral_analytics_event",
        POST_BEHAVIORAL_ANALYTICS_EVENT_PATTERN
    );

    public static final NamedClusterPrivilege CROSS_CLUSTER_SEARCH = new ActionClusterPrivilege(
        "cross_cluster_search",
        CROSS_CLUSTER_SEARCH_PATTERN
    );

    public static final NamedClusterPrivilege CROSS_CLUSTER_REPLICATION = new ActionClusterPrivilege(
        "cross_cluster_replication",
        CROSS_CLUSTER_REPLICATION_PATTERN
    );

    private static final Map<String, NamedClusterPrivilege> VALUES = sortByAccessLevel(
        Stream.of(
            NONE,
            ALL,
            MONITOR,
            MONITOR_ML,
            MONITOR_TEXT_STRUCTURE,
            MONITOR_TRANSFORM_DEPRECATED,
            MONITOR_TRANSFORM,
            MONITOR_WATCHER,
            MONITOR_ROLLUP,
            MANAGE,
            MANAGE_ML,
            MANAGE_TRANSFORM_DEPRECATED,
            MANAGE_TRANSFORM,
            MANAGE_TOKEN,
            MANAGE_WATCHER,
            MANAGE_IDX_TEMPLATES,
            MANAGE_INGEST_PIPELINES,
            READ_PIPELINE,
            TRANSPORT_CLIENT,
            MANAGE_SECURITY,
            READ_SECURITY,
            MANAGE_SAML,
            MANAGE_OIDC,
            MANAGE_API_KEY,
            GRANT_API_KEY,
            MANAGE_SERVICE_ACCOUNT,
            MANAGE_USER_PROFILE,
            MANAGE_PIPELINE,
            MANAGE_ROLLUP,
            MANAGE_AUTOSCALING,
            MANAGE_CCR,
            READ_CCR,
            CREATE_SNAPSHOT,
            MONITOR_SNAPSHOT,
            MANAGE_ILM,
            READ_ILM,
            MANAGE_SLM,
            READ_SLM,
            DELEGATE_PKI,
            MANAGE_OWN_API_KEY,
            MANAGE_ENRICH,
            MANAGE_LOGSTASH_PIPELINES,
            CANCEL_TASK,
            MANAGE_SEARCH_APPLICATION,
            MANAGE_BEHAVIORAL_ANALYTICS,
            POST_BEHAVIORAL_ANALYTICS_EVENT,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? CROSS_CLUSTER_SEARCH : null,
            TcpTransport.isUntrustedRemoteClusterEnabled() ? CROSS_CLUSTER_REPLICATION : null
        ).filter(Objects::nonNull).toList()
    );

    /**
     * Resolves a {@link NamedClusterPrivilege} from a given name if it exists.
     * If the name is a cluster action, then it converts the name to pattern and creates a {@link ActionClusterPrivilege}
     *
     * @param name either {@link ClusterPrivilegeResolver#names()} or cluster action {@link #isClusterAction(String)}
     * @return instance of {@link NamedClusterPrivilege}
     */
    public static NamedClusterPrivilege resolve(String name) {
        name = Objects.requireNonNull(name).toLowerCase(Locale.ROOT);
        if (isClusterAction(name)) {
            return new ActionClusterPrivilege(name, Set.of(actionToPattern(name)));
        }
        final NamedClusterPrivilege fixedPrivilege = VALUES.get(name);
        if (fixedPrivilege != null) {
            return fixedPrivilege;
        }
        String errorMessage = "unknown cluster privilege ["
            + name
            + "]. a privilege must be either "
            + "one of the predefined cluster privilege names ["
            + Strings.collectionToCommaDelimitedString(VALUES.keySet())
            + "] or a pattern over one of the available "
            + "cluster actions";
        logger.debug(errorMessage);
        throw new IllegalArgumentException(errorMessage);

    }

    public static Set<String> names() {
        return Collections.unmodifiableSet(VALUES.keySet());
    }

    public static boolean isClusterAction(String actionName) {
        return ACTION_MATCHER.test(actionName);
    }

    private static String actionToPattern(String text) {
        return text + "*";
    }

    /**
     * Returns the names of privileges that grant the specified action and request, for the given authentication context.
     * @return A collection of names, ordered (to the extent possible) from least privileged (e.g. {@link #MONITOR})
     * to most privileged (e.g. {@link #ALL})
     * @see #sortByAccessLevel(Collection)
     * @see org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission#check(String, TransportRequest, Authentication)
     */
    public static Collection<String> findPrivilegesThatGrant(String action, TransportRequest request, Authentication authentication) {
        return VALUES.entrySet()
            .stream()
            .filter(e -> e.getValue().permission().check(action, request, authentication))
            .map(Map.Entry::getKey)
            .toList();
    }

    /**
     * Sorts the collection of privileges from least-privilege to most-privilege (to the extent possible),
     * returning them in a sorted map keyed by name.
     */
    static SortedMap<String, NamedClusterPrivilege> sortByAccessLevel(Collection<NamedClusterPrivilege> privileges) {
        // How many other privileges does this privilege imply. Those with a higher count are considered to be a higher privilege
        final Map<String, Long> impliesCount = Maps.newMapWithExpectedSize(privileges.size());
        privileges.forEach(
            priv -> impliesCount.put(
                priv.name(),
                privileges.stream().filter(p2 -> p2 != priv && priv.permission().implies(p2.permission())).count()
            )
        );

        final Comparator<String> compare = Comparator.<String>comparingLong(key -> impliesCount.getOrDefault(key, 0L))
            .thenComparing(Comparator.naturalOrder());
        final TreeMap<String, NamedClusterPrivilege> tree = new TreeMap<>(compare);
        privileges.forEach(p -> tree.put(p.name(), p));
        return Collections.unmodifiableSortedMap(tree);
    }

}
