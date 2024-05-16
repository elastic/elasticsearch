/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;
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

    /**
     * RCS 2.0 allows a single API key to define "replication" and "search" blocks. If both are defined, this results in an API key with 2
     * sets of indices permissions. Due to the way API keys (and roles) work across the multiple index permission, the set of index
     * patterns allowed are effectively the most generous of the sets of index patterns since the index patterns are OR'ed together. For
     * example, `foo` OR `*` results in access to `*`. So, if you have "search" access defined as `foo`, but replication access defined
     * as `*`, the API key effectively allows access to index pattern `*`. This means that the access for API keys that define both
     * "search" and "replication", the action names used are the primary means by which we can constrain CCS to the set of "search" indices
     * as well as how we constrain CCR to the set "replication" indices. For example, if "replication" ever allowed access to
     * `indices:data/read/get` for `*` , then the "replication" permissions would effectively enable users of CCS to get documents,
     * even if "search" is never defined in the RCS 2.0 API key. This obviously is not desirable and in practice when both "search" and
     * "replication" are defined the isolation between CCS and CCR is only achieved because the action names for the workflows do not
     * overlap. This test helps to ensure that the actions names used for RCS 2.0 do not bleed over between search and replication.
     */
    public void testRemoteClusterPrivsDoNotOverlap() {
        try {
            Automatons.recordPatterns = true;
            // check that the action patterns for remote CCS are not allowed by remote CCR privileges
            Arrays.stream(CCS_INDICES_PRIVILEGE_NAMES).forEach(ccsPrivilege -> {
                Automaton ccsAutomaton = IndexPrivilege.get(Set.of(ccsPrivilege)).getAutomaton();
                Automatons.getPatterns(ccsAutomaton).forEach(ccsPattern -> {
                    // emulate an action name that could be allowed by a CCS privilege
                    String actionName = ccsPattern.replaceAll("\\*", randomAlphaOfLengthBetween(1, 8));
                    Arrays.stream(CCR_INDICES_PRIVILEGE_NAMES).forEach(ccrPrivileges -> {
                        String errorMessage = String.format(
                            Locale.ROOT,
                            "CCR privilege \"%s\" allows CCS action \"%s\". This could result in an "
                                + "accidental bleeding of permission between RCS 2.0's search and replication index permissions",
                            ccrPrivileges,
                            ccsPattern
                        );
                        assertFalse(errorMessage, IndexPrivilege.get(Set.of(ccrPrivileges)).predicate.test(actionName));
                    });
                });

                // check that the action patterns for remote CCR are not allowed by remote CCS privileges
                Arrays.stream(CCR_INDICES_PRIVILEGE_NAMES).forEach(ccrPrivilege -> {
                    Automaton ccrAutomaton = IndexPrivilege.get(Set.of(ccrPrivilege)).getAutomaton();
                    Automatons.getPatterns(ccrAutomaton).forEach(ccrPattern -> {
                        // emulate an action name that could be allowed by a CCR privilege
                        String actionName = ccrPattern.replaceAll("\\*", randomAlphaOfLengthBetween(1, 8));
                        Arrays.stream(CCS_INDICES_PRIVILEGE_NAMES).forEach(ccsPrivileges -> {
                            if ("indices:data/read/xpack/ccr/shard_changes*".equals(ccrPattern)) {
                                // do nothing, this action is only applicable to CCR workflows and is a moot point if CCS technically has
                                // access to the index pattern for this action granted by CCR
                            } else {
                                String errorMessage = String.format(
                                    Locale.ROOT,
                                    "CCS privilege \"%s\" allows CCR action \"%s\". This could result in an accidental bleeding of "
                                        + "permission between RCS 2.0's search and replication index permissions",
                                    ccsPrivileges,
                                    ccrPattern
                                );
                                assertFalse(errorMessage, IndexPrivilege.get(Set.of(ccsPrivileges)).predicate.test(actionName));
                            }
                        });
                    });
                });
            });
        } finally {
            Automatons.recordPatterns = false;
        }
    }
}
