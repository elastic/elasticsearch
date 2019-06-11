/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetStatusAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.support.Automatons.minusAndMinimize;
import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;

public enum DefaultClusterPrivilege {

    NONE("none", Automatons.EMPTY),
    ALL("all", ClusterAutomatons.ALL_CLUSTER_AUTOMATON),
    MONITOR("monitor", ClusterAutomatons.MONITOR_AUTOMATON),
    MONITOR_ML("monitor_ml", ClusterAutomatons.MONITOR_ML_AUTOMATON),
    MONITOR_DATA_FRAME("monitor_data_frame_transforms", ClusterAutomatons.MONITOR_DATA_FRAME_AUTOMATON),
    MONITOR_WATCHER("monitor_watcher", ClusterAutomatons.MONITOR_WATCHER_AUTOMATON),
    MONITOR_ROLLUP("monitor_rollup", ClusterAutomatons.MONITOR_ROLLUP_AUTOMATON),
    MANAGE("manage", ClusterAutomatons.MANAGE_AUTOMATON),
    MANAGE_ML("manage_ml", ClusterAutomatons.MANAGE_ML_AUTOMATON),
    MANAGE_DATA_FRAME("manage_data_frame_transforms", ClusterAutomatons.MANAGE_DATA_FRAME_AUTOMATON),
    MANAGE_TOKEN("manage_token", ClusterAutomatons.MANAGE_TOKEN_AUTOMATON),
    MANAGE_API_KEY("manage_api_key", ClusterAutomatons.MANAGE_API_KEY_AUTOMATON),
    MANAGE_WATCHER("manage_watcher", ClusterAutomatons.MANAGE_WATCHER_AUTOMATON),
    MANAGE_ROLLUP("manage_rollup", ClusterAutomatons.MANAGE_ROLLUP_AUTOMATON),
    MANAGE_IDX_TEMPLATES("manage_index_templates", ClusterAutomatons.MANAGE_IDX_TEMPLATE_AUTOMATON),
    MANAGE_INGEST_PIPELINES("manage_ingest_pipelines", ClusterAutomatons.MANAGE_INGEST_PIPELINE_AUTOMATON),
    TRANSPORT_CLIENT("transport_client", ClusterAutomatons.TRANSPORT_CLIENT_AUTOMATON),
    MANAGE_SECURITY("manage_security", ClusterAutomatons.MANAGE_SECURITY_AUTOMATON),
    MANAGE_SAML("manage_saml", ClusterAutomatons.MANAGE_SAML_AUTOMATON),
    MANAGE_OIDC("manage_oidc", ClusterAutomatons.MANAGE_OIDC_AUTOMATON),
    MANAGE_PIPELINE("manage_pipeline", "cluster:admin/ingest/pipeline/*"),
    MANAGE_CCR("manage_ccr", ClusterAutomatons.MANAGE_CCR_AUTOMATON),
    READ_CCR("read_ccr", ClusterAutomatons.READ_CCR_AUTOMATON),
    CREATE_SNAPSHOT("create_snapshot", ClusterAutomatons.CREATE_SNAPSHOT_AUTOMATON),
    MANAGE_ILM("manage_ilm", ClusterAutomatons.MANAGE_ILM_AUTOMATON),
    READ_ILM("read_ilm", ClusterAutomatons.READ_ILM_AUTOMATON),
    ;

    private final ClusterPrivilege clusterPrivilege;
    private final String privilegeName;
    private static Map<String, DefaultClusterPrivilege> privilegeNameToEnumMap = new HashMap<>();
    static {
        for (DefaultClusterPrivilege privilege : values()) {
            privilegeNameToEnumMap.put(privilege.privilegeName, privilege);
        }
    }

    DefaultClusterPrivilege(String privilegeName, String clusterAction) {
        this.clusterPrivilege = new ClusterPrivilege(privilegeName, clusterAction);
        this.privilegeName = privilegeName;
    }

    DefaultClusterPrivilege(String privilegeName, Automaton automaton) {
        this.clusterPrivilege = new ClusterPrivilege(privilegeName, automaton);
        this.privilegeName = privilegeName;
    }

    public String privilegeName() {
        return privilegeName;
    }

    public ClusterPrivilege clusterPrivilege() {
        return clusterPrivilege;
    }

    public Predicate<String> predicate() {
        return clusterPrivilege.predicate;
    }

    public Automaton automaton() {
        return clusterPrivilege.automaton;
    }

    public static DefaultClusterPrivilege fromString(String privilegeName) {
        return privilegeNameToEnumMap.get(privilegeName);
    }

    public static Set<String> names() {
        return privilegeNameToEnumMap.keySet();
    }

    static class ClusterAutomatons {
        // shared automatons
        private static final Automaton MANAGE_SECURITY_AUTOMATON = patterns("cluster:admin/xpack/security/*");
        private static final Automaton MANAGE_SAML_AUTOMATON = patterns("cluster:admin/xpack/security/saml/*",
                InvalidateTokenAction.NAME, RefreshTokenAction.NAME);
        private static final Automaton MANAGE_OIDC_AUTOMATON = patterns("cluster:admin/xpack/security/oidc/*");
        private static final Automaton MANAGE_TOKEN_AUTOMATON = patterns("cluster:admin/xpack/security/token/*");

        private static final Automaton MANAGE_API_KEY_AUTOMATON = patterns("cluster:admin/xpack/security/api_key/*");

        private static final Automaton MONITOR_AUTOMATON = patterns("cluster:monitor/*");
        private static final Automaton MONITOR_ML_AUTOMATON = patterns("cluster:monitor/xpack/ml/*");
        private static final Automaton MONITOR_DATA_FRAME_AUTOMATON = patterns("cluster:monitor/data_frame/*");
        private static final Automaton MONITOR_WATCHER_AUTOMATON = patterns("cluster:monitor/xpack/watcher/*");
        private static final Automaton MONITOR_ROLLUP_AUTOMATON = patterns("cluster:monitor/xpack/rollup/*");
        private static final Automaton ALL_CLUSTER_AUTOMATON = patterns("cluster:*", "indices:admin/template/*");
        private static final Automaton MANAGE_AUTOMATON = minusAndMinimize(ALL_CLUSTER_AUTOMATON, MANAGE_SECURITY_AUTOMATON);
        private static final Automaton MANAGE_ML_AUTOMATON = patterns("cluster:admin/xpack/ml/*", "cluster:monitor/xpack/ml/*");
        private static final Automaton MANAGE_DATA_FRAME_AUTOMATON = patterns("cluster:admin/data_frame/*", "cluster:monitor/data_frame/*");
        private static final Automaton MANAGE_WATCHER_AUTOMATON = patterns("cluster:admin/xpack/watcher/*",
                "cluster:monitor/xpack/watcher/*");
        private static final Automaton TRANSPORT_CLIENT_AUTOMATON = patterns("cluster:monitor/nodes/liveness", "cluster:monitor/state");
        private static final Automaton MANAGE_IDX_TEMPLATE_AUTOMATON = patterns("indices:admin/template/*");
        private static final Automaton MANAGE_INGEST_PIPELINE_AUTOMATON = patterns("cluster:admin/ingest/pipeline/*");
        private static final Automaton MANAGE_ROLLUP_AUTOMATON = patterns("cluster:admin/xpack/rollup/*", "cluster:monitor/xpack/rollup/*");
        private static final Automaton MANAGE_CCR_AUTOMATON =
            patterns("cluster:admin/xpack/ccr/*", ClusterStateAction.NAME, HasPrivilegesAction.NAME);
        private static final Automaton CREATE_SNAPSHOT_AUTOMATON = patterns(CreateSnapshotAction.NAME, SnapshotsStatusAction.NAME + "*",
                GetSnapshotsAction.NAME, SnapshotsStatusAction.NAME, GetRepositoriesAction.NAME);
        private static final Automaton READ_CCR_AUTOMATON = patterns(ClusterStateAction.NAME, HasPrivilegesAction.NAME);
        private static final Automaton MANAGE_ILM_AUTOMATON = patterns("cluster:admin/ilm/*");
        private static final Automaton READ_ILM_AUTOMATON = patterns(GetLifecycleAction.NAME, GetStatusAction.NAME);
    }
}
