/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege.findPrivilegesThatGrant;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class IndexPrivilegeTests extends ESTestCase {

    /**
     * The {@link IndexPrivilege#values()} map is sorted so that privilege names that offer the _least_ access come before those that
     * offer _more_ access. There is no guarantee of ordering between privileges that offer non-overlapping privileges.
     */
    public void testOrderingOfPrivilegeNames() throws Exception {
        final Set<String> names = IndexPrivilege.values().keySet();
        final int all = Iterables.indexOf(names, "all"::equals);
        final int manage = Iterables.indexOf(names, "manage"::equals);
        final int monitor = Iterables.indexOf(names, "monitor"::equals);
        final int read = Iterables.indexOf(names, "read"::equals);
        final int write = Iterables.indexOf(names, "write"::equals);
        final int index = Iterables.indexOf(names, "index"::equals);
        final int createDoc = Iterables.indexOf(names, "create_doc"::equals);
        final int delete = Iterables.indexOf(names, "delete"::equals);
        final int viewIndexMetadata = Iterables.indexOf(names, "view_index_metadata"::equals);

        assertThat(read, lessThan(all));
        assertThat(manage, lessThan(all));
        assertThat(monitor, lessThan(manage));
        assertThat(write, lessThan(all));
        assertThat(index, lessThan(write));
        assertThat(createDoc, lessThan(index));
        assertThat(delete, lessThan(write));
        assertThat(viewIndexMetadata, lessThan(manage));
    }

    public void testFindPrivilegesThatGrant() {
        assertThat(findPrivilegesThatGrant(TransportSearchAction.TYPE.name()), equalTo(List.of("read", "all")));
        assertThat(findPrivilegesThatGrant(TransportIndexAction.NAME), equalTo(List.of("create_doc", "create", "index", "write", "all")));
        assertThat(findPrivilegesThatGrant(TransportUpdateAction.NAME), equalTo(List.of("index", "write", "all")));
        assertThat(findPrivilegesThatGrant(TransportDeleteAction.NAME), equalTo(List.of("delete", "write", "all")));
        assertThat(
            findPrivilegesThatGrant(IndicesStatsAction.NAME),
            equalTo(List.of("monitor", "cross_cluster_replication", "manage", "all"))
        );
        assertThat(findPrivilegesThatGrant(RefreshAction.NAME), equalTo(List.of("maintenance", "manage", "all")));
    }

    public void testGetSingleSelector() {
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("all"));
            assertThat(actual, equalTo(IndexPrivilege.ALL));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.ALL));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("read"));
            assertThat(actual, equalTo(IndexPrivilege.READ));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.DATA));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("none"));
            assertThat(actual, equalTo(IndexPrivilege.NONE));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.DATA));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of());
            assertThat(actual, equalTo(IndexPrivilege.NONE));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.DATA));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("indices:data/read/search"));
            assertThat(actual.getSingleName(), equalTo("indices:data/read/search"));
            assertThat(actual.predicate.test("indices:data/read/search"), is(true));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.DATA));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("all", "read", "indices:data/read/search"));
            assertThat(actual.name, equalTo(Set.of("all", "read", "indices:data/read/search")));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.ALL));
        }
    }

    public void testGetSingleSelectorWithFailuresSelector() {
        assumeTrue("This test requires the failure store to be enabled", DataStream.isFailureStoreFeatureFlagEnabled());
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("read_failure_store"));
            assertThat(actual, equalTo(IndexPrivilege.READ_FAILURE_STORE));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.FAILURES));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("all", "read_failure_store"));
            assertThat(actual.name(), equalTo(Set.of("all", "read_failure_store")));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.ALL));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("all", "indices:data/read/search", "read_failure_store"));
            assertThat(actual.name(), equalTo(Set.of("all", "indices:data/read/search", "read_failure_store")));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.ALL));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
        }
        {
            IndexPrivilege actual = IndexPrivilege.getSingleSelector(Set.of("all", "read", "read_failure_store"));
            assertThat(actual.name(), equalTo(Set.of("all", "read", "read_failure_store")));
            assertThat(actual.getSelectorPrivilege(), equalTo(IndexComponentSelectorPrivilege.ALL));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
        }
        expectThrows(IllegalArgumentException.class, () -> IndexPrivilege.getSingleSelector(Set.of("read", "read_failure_store")));
        expectThrows(
            IllegalArgumentException.class,
            () -> IndexPrivilege.getSingleSelector(Set.of("indices:data/read/search", "read_failure_store"))
        );
        expectThrows(IllegalArgumentException.class, () -> IndexPrivilege.getSingleSelector(Set.of("none", "read_failure_store")));
    }

    public void testPrivilegesForRollupFieldCapsAction() {
        final Collection<String> privileges = findPrivilegesThatGrant(GetRollupIndexCapsAction.NAME);
        assertThat(Set.copyOf(privileges), equalTo(Set.of("manage", "all", "view_index_metadata", "read")));
    }

    public void testPrivilegesForGetCheckPointAction() {
        assertThat(
            findPrivilegesThatGrant(GetCheckpointAction.NAME),
            containsInAnyOrder("monitor", "view_index_metadata", "manage", "all")
        );
    }

    public void testRelationshipBetweenPrivileges() {
        assertThat(
            Automatons.subsetOf(
                IndexPrivilege.getSingleSelector(Set.of("view_index_metadata")).automaton,
                IndexPrivilege.getSingleSelector(Set.of("manage")).automaton
            ),
            is(true)
        );

        assertThat(
            Automatons.subsetOf(
                IndexPrivilege.getSingleSelector(Set.of("monitor")).automaton,
                IndexPrivilege.getSingleSelector(Set.of("manage")).automaton
            ),
            is(true)
        );

        assertThat(
            Automatons.subsetOf(
                IndexPrivilege.getSingleSelector(Set.of("create", "create_doc", "index", "delete")).automaton,
                IndexPrivilege.getSingleSelector(Set.of("write")).automaton
            ),
            is(true)
        );

        assertThat(
            Automatons.subsetOf(
                IndexPrivilege.getSingleSelector(Set.of("create_index", "delete_index")).automaton,
                IndexPrivilege.getSingleSelector(Set.of("manage")).automaton
            ),
            is(true)
        );
    }

    public void testCrossClusterReplicationPrivileges() {
        final IndexPrivilege crossClusterReplication = IndexPrivilege.getSingleSelector(Set.of("cross_cluster_replication"));
        List.of(
            "indices:data/read/xpack/ccr/shard_changes",
            "indices:monitor/stats",
            "indices:admin/seq_no/add_retention_lease",
            "indices:admin/seq_no/remove_retention_lease",
            "indices:admin/seq_no/renew_retention_lease"
        ).forEach(action -> assertThat(crossClusterReplication.predicate.test(action + randomAlphaOfLengthBetween(0, 8)), is(true)));
        assertThat(
            Automatons.subsetOf(
                crossClusterReplication.automaton,
                IndexPrivilege.getSingleSelector(Set.of("manage", "read", "monitor")).automaton
            ),
            is(true)
        );

        final IndexPrivilege crossClusterReplicationInternal = IndexPrivilege.getSingleSelector(
            Set.of("cross_cluster_replication_internal")
        );
        List.of(
            "indices:internal/admin/ccr/restore/session/clear",
            "indices:internal/admin/ccr/restore/file_chunk/get",
            "indices:internal/admin/ccr/restore/session/put",
            "internal:transport/proxy/indices:internal/admin/ccr/restore/session/clear",
            "internal:transport/proxy/indices:internal/admin/ccr/restore/file_chunk/get"
        )
            .forEach(
                action -> assertThat(crossClusterReplicationInternal.predicate.test(action + randomAlphaOfLengthBetween(0, 8)), is(true))
            );

        assertThat(
            Automatons.subsetOf(crossClusterReplicationInternal.automaton, IndexPrivilege.getSingleSelector(Set.of("manage")).automaton),
            is(false)
        );
        assertThat(
            Automatons.subsetOf(crossClusterReplicationInternal.automaton, IndexPrivilege.getSingleSelector(Set.of("all")).automaton),
            is(true)
        );
    }

}
