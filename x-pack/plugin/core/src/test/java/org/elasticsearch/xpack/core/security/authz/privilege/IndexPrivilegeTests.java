/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

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
        assertThat(findPrivilegesThatGrant(SearchAction.NAME), equalTo(List.of("read", "all")));
        assertThat(findPrivilegesThatGrant(IndexAction.NAME), equalTo(List.of("create_doc", "create", "index", "write", "all")));
        assertThat(findPrivilegesThatGrant(UpdateAction.NAME), equalTo(List.of("index", "write", "all")));
        assertThat(findPrivilegesThatGrant(DeleteAction.NAME), equalTo(List.of("delete", "write", "all")));
        assertThat(
            findPrivilegesThatGrant(IndicesStatsAction.NAME),
            equalTo(
                Stream.of("monitor", (TcpTransport.isUntrustedRemoteClusterEnabled() ? "cross_cluster_replication" : null), "manage", "all")
                    .filter(Objects::nonNull)
                    .toList()
            )
        );
        assertThat(findPrivilegesThatGrant(RefreshAction.NAME), equalTo(List.of("maintenance", "manage", "all")));
        assertThat(findPrivilegesThatGrant(ShrinkAction.NAME), equalTo(List.of("manage", "all")));
    }

    public void testPrivilegesForRollupFieldCapsAction() {
        final Collection<String> privileges = findPrivilegesThatGrant(GetRollupIndexCapsAction.NAME);
        assertThat(Set.copyOf(privileges), equalTo(Set.of("read", "view_index_metadata", "manage", "all")));
    }

    public void testPrivilegesForGetCheckPointAction() {
        assertThat(
            findPrivilegesThatGrant(GetCheckpointAction.NAME),
            containsInAnyOrder("monitor", "view_index_metadata", "manage", "all")
        );
    }

    public void testRelationshipBetweenPrivileges() {
        assertThat(
            Operations.subsetOf(
                IndexPrivilege.get(Set.of("view_index_metadata")).automaton,
                IndexPrivilege.get(Set.of("manage")).automaton
            ),
            is(true)
        );

        assertThat(
            Operations.subsetOf(IndexPrivilege.get(Set.of("monitor")).automaton, IndexPrivilege.get(Set.of("manage")).automaton),
            is(true)
        );

        assertThat(
            Operations.subsetOf(
                IndexPrivilege.get(Set.of("create", "create_doc", "index", "delete")).automaton,
                IndexPrivilege.get(Set.of("write")).automaton
            ),
            is(true)
        );

        assertThat(
            Operations.subsetOf(
                IndexPrivilege.get(Set.of("create_index", "delete_index")).automaton,
                IndexPrivilege.get(Set.of("manage")).automaton
            ),
            is(true)
        );
    }

    public void testCrossClusterReplicationPrivileges() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final IndexPrivilege crossClusterReplication = IndexPrivilege.get(Set.of("cross_cluster_replication"));
        List.of(
            "indices:data/read/xpack/ccr/shard_changes",
            "indices:monitor/stats",
            "indices:admin/seq_no/add_retention_lease",
            "indices:admin/seq_no/remove_retention_lease",
            "indices:admin/seq_no/renew_retention_lease"
        ).forEach(action -> assertThat(crossClusterReplication.predicate.test(action + randomAlphaOfLengthBetween(0, 8)), is(true)));
        assertThat(
            Operations.subsetOf(crossClusterReplication.automaton, IndexPrivilege.get(Set.of("manage", "read", "monitor")).automaton),
            is(true)
        );

        final IndexPrivilege crossClusterReplicationInternal = IndexPrivilege.get(Set.of("cross_cluster_replication_internal"));
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
            Operations.subsetOf(crossClusterReplicationInternal.automaton, IndexPrivilege.get(Set.of("manage")).automaton),
            is(false)
        );
        assertThat(Operations.subsetOf(crossClusterReplicationInternal.automaton, IndexPrivilege.get(Set.of("all")).automaton), is(true));
    }
}
