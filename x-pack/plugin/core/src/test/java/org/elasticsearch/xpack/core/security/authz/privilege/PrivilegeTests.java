/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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

    public void testCluster() throws Exception {
        Set<String> name = Sets.newHashSet("monitor");
        ClusterPrivilege cluster = ClusterPrivilege.get(name);
        assertThat(cluster, is(ClusterPrivilege.MONITOR));

        // since "all" implies "monitor", this should be the same language as All
        name = Sets.newHashSet("monitor", "all");
        cluster = ClusterPrivilege.get(name);
        assertTrue(Operations.sameLanguage(ClusterPrivilege.ALL.automaton, cluster.automaton));

        name = Sets.newHashSet("monitor", "none");
        cluster = ClusterPrivilege.get(name);
        assertTrue(Operations.sameLanguage(ClusterPrivilege.MONITOR.automaton, cluster.automaton));

        Set<String> name2 = Sets.newHashSet("none", "monitor");
        ClusterPrivilege cluster2 = ClusterPrivilege.get(name2);
        assertThat(cluster, is(cluster2));
    }

    public void testClusterTemplateActions() throws Exception {
        Set<String> name = Sets.newHashSet("indices:admin/template/delete");
        ClusterPrivilege cluster = ClusterPrivilege.get(name);
        assertThat(cluster, notNullValue());
        assertThat(cluster.predicate().test("indices:admin/template/delete"), is(true));

        name = Sets.newHashSet("indices:admin/template/get");
        cluster = ClusterPrivilege.get(name);
        assertThat(cluster, notNullValue());
        assertThat(cluster.predicate().test("indices:admin/template/get"), is(true));

        name = Sets.newHashSet("indices:admin/template/put");
        cluster = ClusterPrivilege.get(name);
        assertThat(cluster, notNullValue());
        assertThat(cluster.predicate().test("indices:admin/template/put"), is(true));
    }

    public void testClusterInvalidName() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        Set<String> actionName = Sets.newHashSet("foobar");
        ClusterPrivilege.get(actionName);
    }

    public void testClusterAction() throws Exception {
        Set<String> actionName = Sets.newHashSet("cluster:admin/snapshot/delete");
        ClusterPrivilege cluster = ClusterPrivilege.get(actionName);
        assertThat(cluster, notNullValue());
        assertThat(cluster.predicate().test("cluster:admin/snapshot/delete"), is(true));
        assertThat(cluster.predicate().test("cluster:admin/snapshot/dele"), is(false));
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
        assertThat(predicate.test("indices:admin/seq_no/global_checkpoint_sync"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/global_checkpoint_sync[p]"), is(true));
        assertThat(predicate.test("indices:admin/seq_no/global_checkpoint_sync[r]"), is(true));
        assertThat(predicate.test("indices:admin/settings/update"), is(true));
        assertThat(predicate.test("indices:admin/settings/foo"), is(false));
    }

    public void testManageCcrPrivilege() {
        Predicate<String> predicate = ClusterPrivilege.MANAGE_CCR.predicate();
        assertThat(predicate.test("cluster:admin/xpack/ccr/follow_index"), is(true));
        assertThat(predicate.test("cluster:admin/xpack/ccr/unfollow_index"), is(true));
        assertThat(predicate.test("cluster:admin/xpack/ccr/brand_new_api"), is(true));
        assertThat(predicate.test("cluster:admin/xpack/whatever"), is(false));
    }

    public void testIlmPrivileges() {
        {
            Predicate<String> predicate = ClusterPrivilege.MANAGE_ILM.predicate();
            // check cluster actions
            assertThat(predicate.test("cluster:admin/ilm/delete"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/_move/post"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/put"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/start"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/stop"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/brand_new_api"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/get"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/operation_mode/get"), is(true));
            // check non-ilm action
            assertThat(predicate.test("cluster:admin/whatever"), is(false));
        }

        {
            Predicate<String> predicate = ClusterPrivilege.READ_ILM.predicate();
            // check cluster actions
            assertThat(predicate.test("cluster:admin/ilm/delete"), is(false));
            assertThat(predicate.test("cluster:admin/ilm/_move/post"), is(false));
            assertThat(predicate.test("cluster:admin/ilm/put"), is(false));
            assertThat(predicate.test("cluster:admin/ilm/start"), is(false));
            assertThat(predicate.test("cluster:admin/ilm/stop"), is(false));
            assertThat(predicate.test("cluster:admin/ilm/brand_new_api"), is(false));
            assertThat(predicate.test("cluster:admin/ilm/get"), is(true));
            assertThat(predicate.test("cluster:admin/ilm/operation_mode/get"), is(true));
            // check non-ilm action
            assertThat(predicate.test("cluster:admin/whatever"), is(false));
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
}
