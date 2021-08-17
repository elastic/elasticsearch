/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class PrivilegeTests extends ESTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public void testSubActionPattern() throws Exception {
        Predicate<String> predicate = Automatons.predicate("foo*");
        assertThat(predicate.test("foo[n][nodes]"), is(true));
        assertThat(predicate.test("foo[n]"), is(true));
        assertThat(predicate.test("bar[n][nodes]"), is(false));
        assertThat(predicate.test("[n][nodes]"), is(false));
    }
    private void verifyClusterActionAllowed(ClusterPrivilege clusterPrivilege, String... actions) {
        ClusterPermission clusterPermission = clusterPrivilege.buildPermission(ClusterPermission.builder()).build();
        for (String action: actions) {
            assertTrue(clusterPermission.check(action, mock(TransportRequest.class), mock(Authentication.class)));
        }
    }
    private void verifyClusterActionDenied(ClusterPrivilege clusterPrivilege, String... actions) {
        ClusterPermission clusterPermission = clusterPrivilege.buildPermission(ClusterPermission.builder()).build();
        for (String action: actions) {
            assertFalse(clusterPermission.check(action, mock(TransportRequest.class), mock(Authentication.class)));
        }
    }
    public void testCluster() throws Exception {
        ClusterPrivilege allClusterPrivilege = ClusterPrivilegeResolver.resolve("all");
        assertThat(allClusterPrivilege, is(ClusterPrivilegeResolver.ALL));
        verifyClusterActionAllowed(allClusterPrivilege, "cluster:admin/xpack/security/*");

        ClusterPrivilege monitorClusterPrivilege = ClusterPrivilegeResolver.resolve("monitor");
        assertThat(monitorClusterPrivilege, is(ClusterPrivilegeResolver.MONITOR));
        verifyClusterActionAllowed(monitorClusterPrivilege, "cluster:monitor/*");
        verifyClusterActionDenied(monitorClusterPrivilege, "cluster:admin/xpack/security/*");

        ClusterPrivilege noneClusterPrivilege = ClusterPrivilegeResolver.resolve("none");
        assertThat(noneClusterPrivilege, is(ClusterPrivilegeResolver.NONE));
        verifyClusterActionDenied(noneClusterPrivilege, "cluster:admin/xpack/security/*");
        verifyClusterActionDenied(noneClusterPrivilege, "cluster:monitor/*");
        verifyClusterActionDenied(noneClusterPrivilege, "*");

        ClusterPermission monitorClusterPermission = monitorClusterPrivilege.buildPermission(ClusterPermission.builder()).build();
        ClusterPermission allClusterPermission = allClusterPrivilege.buildPermission(ClusterPermission.builder()).build();

        // all implies monitor
        assertTrue(allClusterPermission.implies(monitorClusterPermission));

        ClusterPermission.Builder builder = ClusterPermission.builder();
        builder = allClusterPrivilege.buildPermission(builder);
        builder = noneClusterPrivilege.buildPermission(builder);
        ClusterPermission combinedPermission = builder.build();
        assertTrue(combinedPermission.implies(monitorClusterPermission));
    }

    public void testClusterTemplateActions() throws Exception {
        ClusterPrivilege clusterPrivilegeTemplateDelete = ClusterPrivilegeResolver.resolve("indices:admin/template/delete");
        assertThat(clusterPrivilegeTemplateDelete, notNullValue());
        verifyClusterActionAllowed(clusterPrivilegeTemplateDelete, "indices:admin/template/delete");
        verifyClusterActionDenied(clusterPrivilegeTemplateDelete, "indices:admin/template/get", "indices:admin/template/put");

        ClusterPrivilege clusterPrivilegeTemplateGet = ClusterPrivilegeResolver.resolve("indices:admin/template/get");
        assertThat(clusterPrivilegeTemplateGet, notNullValue());
        verifyClusterActionAllowed(clusterPrivilegeTemplateGet, "indices:admin/template/get");
        verifyClusterActionDenied(clusterPrivilegeTemplateGet, "indices:admin/template/delete", "indices:admin/template/put");

        ClusterPrivilege clusterPrivilegeTemplatePut = ClusterPrivilegeResolver.resolve("indices:admin/template/put");
        assertThat(clusterPrivilegeTemplatePut, notNullValue());
        verifyClusterActionAllowed(clusterPrivilegeTemplatePut, "indices:admin/template/put");
        verifyClusterActionDenied(clusterPrivilegeTemplatePut, "indices:admin/template/get", "indices:admin/template/delete");
    }

    public void testClusterInvalidName() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        ClusterPrivilegeResolver.resolve("foobar");

    }

    public void testClusterAction() throws Exception {
        // ClusterPrivilegeResolver.resolve() for a cluster action converts action name into a pattern by adding "*"
        ClusterPrivilege clusterPrivilegeSnapshotDelete = ClusterPrivilegeResolver.resolve("cluster:admin/snapshot/delete");
        assertThat(clusterPrivilegeSnapshotDelete, notNullValue());
        verifyClusterActionAllowed(clusterPrivilegeSnapshotDelete, "cluster:admin/snapshot/delete", "cluster:admin/snapshot/delete[n]",
            "cluster:admin/snapshot/delete/non-existing");
        verifyClusterActionDenied(clusterPrivilegeSnapshotDelete, "cluster:admin/snapshot/dele", "cluster:admin/snapshot/dele[n]",
            "cluster:admin/snapshot/dele/non-existing");
    }

    public void testIndexAction() throws Exception {
        Set<String> actionName = Sets.newHashSet("indices:admin/mapping/delete");
        IndexPrivilege index = IndexPrivilege.get(actionName);
        assertThat(index, notNullValue());
        assertThat(index.predicate().test("indices:admin/mapping/delete"), is(true));
        assertThat(index.predicate().test("indices:admin/mapping/dele"), is(false));
        assertThat(IndexPrivilege.READ_CROSS_CLUSTER.predicate()
                .test("internal:transport/proxy/indices:data/read/query"), is(true));
    }

    public void testIndexCollapse() throws Exception {
        IndexPrivilege[] values = IndexPrivilege.values().values().toArray(new IndexPrivilege[IndexPrivilege.values().size()]);
        IndexPrivilege first = values[randomIntBetween(0, values.length-1)];
        IndexPrivilege second = values[randomIntBetween(0, values.length-1)];

        Set<String> name = Sets.newHashSet(first.name().iterator().next(), second.name().iterator().next());
        IndexPrivilege index = IndexPrivilege.get(name);

        if (Operations.subsetOf(second.getAutomaton(), first.getAutomaton())) {
            assertTrue(Operations.sameLanguage(index.getAutomaton(), first.getAutomaton()));
        } else if (Operations.subsetOf(first.getAutomaton(), second.getAutomaton())) {
            assertTrue(Operations.sameLanguage(index.getAutomaton(), second.getAutomaton()));
        } else {
            assertFalse(Operations.sameLanguage(index.getAutomaton(), first.getAutomaton()));
            assertFalse(Operations.sameLanguage(index.getAutomaton(), second.getAutomaton()));
        }
    }

    public void testSystem() throws Exception {
        Predicate<String> predicate = SystemPrivilege.INSTANCE.predicate();
        assertThat(predicate.test("indices:monitor/whatever"), is(true));
        assertThat(predicate.test("cluster:monitor/whatever"), is(true));
        assertThat(predicate.test("cluster:admin/snapshot/status[nodes]"), is(false));
        assertThat(predicate.test("internal:whatever"), is(true));
        assertThat(predicate.test("indices:whatever"), is(false));
        assertThat(predicate.test("cluster:whatever"), is(false));
        assertThat(predicate.test("cluster:admin/snapshot/status"), is(false));
        assertThat(predicate.test("whatever"), is(false));
        assertThat(predicate.test("cluster:admin/reroute"), is(true));
        assertThat(predicate.test("cluster:admin/whatever"), is(false));
        assertThat(predicate.test("indices:admin/mapping/put"), is(true));
        assertThat(predicate.test("indices:admin/mapping/whatever"), is(false));
        assertThat(predicate.test("internal:transport/proxy/indices:data/read/query"), is(false));
        assertThat(predicate.test("internal:transport/proxy/indices:monitor/whatever"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/global_checkpoint_sync"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/global_checkpoint_sync[p]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/global_checkpoint_sync[r]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/retention_lease_sync"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/retention_lease_sync[p]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/retention_lease_sync[r]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/retention_lease_background_sync"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/retention_lease_background_sync[p]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/retention_lease_background_sync[r]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/add_retention_lease"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/add_retention_lease[s]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/remove_retention_lease"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/remove_retention_lease[s]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/renew_retention_lease"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/renew_retention_lease[s]"), is(true));
        assertThat(predicate.test("indices:admin/settings/update"), is(true));
        assertThat(predicate.test("indices:admin/settings/foo"), is(false));
        assertThat(predicate.test("cluster:admin/transform/stop"), is(true));
        assertThat(predicate.test("cluster:admin/xpack/ml/data_frame/analytics/stop"), is(true));
        assertThat(predicate.test("cluster:admin/xpack/ml/datafeed/stop"), is(true));
        assertThat(predicate.test("cluster:admin/xpack/ml/job/close"), is(true));
        assertThat(predicate.test("cluster:internal/xpack/ml/job/kill/process[n]"), is(true));
        assertThat(predicate.test("cluster:internal/xpack/ml/reset_mode"), is(true));
        assertThat(predicate.test("cluster:internal/xpack/transform/reset_mode"), is(true));
        assertThat(predicate.test("indices:admin/data_stream/delete"), is(true));
        assertThat(predicate.test("indices:admin/delete"), is(true));
    }

    public void testManageAutoscalingPrivilege() {
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_AUTOSCALING, "cluster:admin/autoscaling/get_decision");
    }

    public void testManageCcrPrivilege() {
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_CCR, "cluster:admin/xpack/ccr/follow_index",
            "cluster:admin/xpack/ccr/unfollow_index", "cluster:admin/xpack/ccr/brand_new_api");
        verifyClusterActionDenied(ClusterPrivilegeResolver.MANAGE_CCR, "cluster:admin/xpack/whatever");

    }

    public void testManageEnrichPrivilege() {
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_ENRICH, DeleteEnrichPolicyAction.NAME);
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_ENRICH, ExecuteEnrichPolicyAction.NAME);
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_ENRICH, GetEnrichPolicyAction.NAME);
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_ENRICH, PutEnrichPolicyAction.NAME);
        verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_ENRICH, "cluster:admin/xpack/enrich/brand_new_api");
        verifyClusterActionDenied(ClusterPrivilegeResolver.MANAGE_ENRICH, "cluster:admin/xpack/whatever");
    }

    public void testIlmPrivileges() {
        {
            verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_ILM, "cluster:admin/ilm/delete",
                "cluster:admin/ilm/_move/post", "cluster:admin/ilm/put", "cluster:admin/ilm/start",
                "cluster:admin/ilm/stop", "cluster:admin/ilm/brand_new_api", "cluster:admin/ilm/get",
                "cluster:admin/ilm/operation_mode/get"
            );
            verifyClusterActionDenied(ClusterPrivilegeResolver.MANAGE_ILM, "cluster:admin/whatever");

        }

        {
            verifyClusterActionAllowed(ClusterPrivilegeResolver.READ_ILM, "cluster:admin/ilm/get", "cluster:admin/ilm/operation_mode/get");
            verifyClusterActionDenied(ClusterPrivilegeResolver.READ_ILM, "cluster:admin/ilm/delete", "cluster:admin/ilm/_move/post",
                "cluster:admin/ilm/put", "cluster:admin/ilm/start", "cluster:admin/ilm/stop",
                "cluster:admin/ilm/brand_new_api", "cluster:admin/whatever");
        }

        {
            Predicate<String> predicate = IndexPrivilege.MANAGE_ILM.predicate();
            // check indices actions
            assertThat(predicate.test("indices:admin/ilm/retry"), is(true));
            assertThat(predicate.test("indices:admin/ilm/remove_policy"), is(true));
            assertThat(predicate.test("indices:admin/ilm/brand_new_api"), is(true));
            assertThat(predicate.test("indices:admin/ilm/explain"), is(true));
            // check non-ilm action
            assertThat(predicate.test("indices:admin/whatever"), is(false));
        }

        {
            Predicate<String> predicate = IndexPrivilege.VIEW_METADATA.predicate();
            // check indices actions
            assertThat(predicate.test("indices:admin/ilm/retry"), is(false));
            assertThat(predicate.test("indices:admin/ilm/remove_policy"), is(false));
            assertThat(predicate.test("indices:admin/ilm/brand_new_api"), is(false));
            assertThat(predicate.test("indices:admin/ilm/explain"), is(true));
            // check non-ilm action
            assertThat(predicate.test("indices:admin/whatever"), is(false));
        }
    }

    public void testSlmPrivileges() {
        {
            verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_SLM, "cluster:admin/slm/delete",
                "cluster:admin/slm/put",
                "cluster:admin/slm/get",
                "cluster:admin/ilm/start",
                "cluster:admin/ilm/stop",
                "cluster:admin/slm/execute",
                "cluster:admin/ilm/operation_mode/get");
            verifyClusterActionDenied(ClusterPrivilegeResolver.MANAGE_SLM, "cluster:admin/whatever");
        }

        {
            verifyClusterActionAllowed(ClusterPrivilegeResolver.READ_SLM,
                "cluster:admin/slm/get",
                "cluster:admin/ilm/operation_mode/get");
            verifyClusterActionDenied(ClusterPrivilegeResolver.READ_SLM,"cluster:admin/slm/delete",
                "cluster:admin/slm/put",
                "cluster:admin/ilm/start",
                "cluster:admin/ilm/stop",
                "cluster:admin/slm/execute",
                "cluster:admin/whatever");

        }
    }

    public void testIngestPipelinePrivileges() {
        {
            verifyClusterActionAllowed(ClusterPrivilegeResolver.MANAGE_INGEST_PIPELINES, "cluster:admin/ingest/pipeline/get",
                "cluster:admin/ingest/pipeline/put",
                "cluster:admin/ingest/pipeline/delete",
                "cluster:admin/ingest/pipeline/simulate");
            verifyClusterActionDenied(ClusterPrivilegeResolver.MANAGE_INGEST_PIPELINES, "cluster:admin/whatever");
        }

        {
            verifyClusterActionAllowed(ClusterPrivilegeResolver.READ_PIPELINE,
                "cluster:admin/ingest/pipeline/get",
                "cluster:admin/ingest/pipeline/simulate");
            verifyClusterActionDenied(ClusterPrivilegeResolver.READ_PIPELINE,"cluster:admin/ingest/pipeline/put",
                "cluster:admin/ingest/pipeline/delete",
                "cluster:admin/whatever");

        }
    }

    public void testCancelTasksPrivilege() {
        verifyClusterActionAllowed(ClusterPrivilegeResolver.CANCEL_TASK, CancelTasksAction.NAME);
        verifyClusterActionAllowed(ClusterPrivilegeResolver.CANCEL_TASK, CancelTasksAction.NAME + "[n]");
        verifyClusterActionDenied(ClusterPrivilegeResolver.CANCEL_TASK, "cluster:admin/whatever");
    }
}
