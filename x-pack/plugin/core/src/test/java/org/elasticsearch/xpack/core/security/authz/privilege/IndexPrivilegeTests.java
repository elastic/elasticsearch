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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

        Predicate<IndexPrivilege> failuresOnly = p -> p.getSelectorPredicate() == IndexComponentSelectorPredicate.FAILURES;
        assertThat(findPrivilegesThatGrant(TransportSearchAction.TYPE.name(), failuresOnly), equalTo(List.of("read_failure_store")));
        assertThat(findPrivilegesThatGrant(TransportIndexAction.NAME, failuresOnly), equalTo(List.of()));
        assertThat(findPrivilegesThatGrant(TransportUpdateAction.NAME, failuresOnly), equalTo(List.of()));
        assertThat(findPrivilegesThatGrant(TransportDeleteAction.NAME, failuresOnly), equalTo(List.of()));
        assertThat(findPrivilegesThatGrant(IndicesStatsAction.NAME, failuresOnly), equalTo(List.of("manage_failure_store")));
        assertThat(findPrivilegesThatGrant(RefreshAction.NAME, failuresOnly), equalTo(List.of("manage_failure_store")));
    }

    public void testGet() {
        {
            IndexPrivilege actual = IndexPrivilege.get("all");
            assertThat(actual, equalTo(IndexPrivilege.ALL));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.ALL));
        }
        {
            IndexPrivilege actual = IndexPrivilege.get("read");
            assertThat(actual, equalTo(IndexPrivilege.READ));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.DATA));
        }
        {
            IndexPrivilege actual = IndexPrivilege.get("none");
            assertThat(actual, equalTo(IndexPrivilege.NONE));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.DATA));
        }
        {
            IndexPrivilege actual = resolvePrivilegeAndAssertSingleton(Set.of());
            assertThat(actual, equalTo(IndexPrivilege.NONE));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.DATA));
        }
        {
            IndexPrivilege actual = IndexPrivilege.get("indices:data/read/search");
            assertThat(actual.name, containsInAnyOrder("indices:data/read/search"));
            assertThat(actual.predicate.test("indices:data/read/search"), is(true));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.DATA));
        }
        {
            IndexPrivilege actual = resolvePrivilegeAndAssertSingleton(Set.of("all", "read", "indices:data/read/search"));
            assertThat(actual.name, equalTo(Set.of("all", "read", "indices:data/read/search")));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.ALL));
        }
    }

    public void testResolveSameSelectorPrivileges() {
        {
            IndexPrivilege actual = resolvePrivilegeAndAssertSingleton(Set.of("read_failure_store"));
            assertThat(actual, equalTo(IndexPrivilege.READ_FAILURE_STORE));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.FAILURES));
        }
        {
            IndexPrivilege actual = resolvePrivilegeAndAssertSingleton(Set.of("all", "read_failure_store"));
            assertThat(actual.name(), equalTo(Set.of("all", "read_failure_store")));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.ALL));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
        }
        {
            IndexPrivilege actual = resolvePrivilegeAndAssertSingleton(Set.of("all", "indices:data/read/search", "read_failure_store"));
            assertThat(actual.name(), equalTo(Set.of("all", "indices:data/read/search", "read_failure_store")));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.ALL));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
        }
        {
            IndexPrivilege actual = resolvePrivilegeAndAssertSingleton(Set.of("all", "read", "read_failure_store"));
            assertThat(actual.name(), equalTo(Set.of("all", "read", "read_failure_store")));
            assertThat(actual.getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.ALL));
            assertThat(Automatons.subsetOf(IndexPrivilege.ALL.automaton, actual.automaton), is(true));
        }
    }

    public void testResolveBySelectorAccess() {
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(Set.of("read_failure_store"));
            assertThat(actual, containsInAnyOrder(IndexPrivilege.READ_FAILURE_STORE));
            assertThat(actual.iterator().next().getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.FAILURES));
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(Set.of("read_failure_store", "READ_FAILURE_STORE"));
            assertThat(actual, containsInAnyOrder(IndexPrivilege.READ_FAILURE_STORE));
            assertThat(actual.iterator().next().getSelectorPredicate(), equalTo(IndexComponentSelectorPredicate.FAILURES));
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(Set.of("read_failure_store", "read", "READ_FAILURE_STORE"));
            assertThat(actual, containsInAnyOrder(IndexPrivilege.READ_FAILURE_STORE, IndexPrivilege.READ));
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.FAILURES)
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(
                Set.of("read_failure_store", "read", "view_index_metadata")
            );
            assertThat(
                actual,
                containsInAnyOrder(
                    IndexPrivilege.READ_FAILURE_STORE,
                    resolvePrivilegeAndAssertSingleton(Set.of("read", "view_index_metadata"))
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.FAILURES)
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(Set.of("read_failure_store", "indices:data/read/*"));
            assertThat(
                actual,
                containsInAnyOrder(IndexPrivilege.READ_FAILURE_STORE, resolvePrivilegeAndAssertSingleton(Set.of("indices:data/read/*")))
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.FAILURES)
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(Set.of("read_failure_store", "indices:data/read/search"));
            assertThat(
                actual,
                containsInAnyOrder(
                    IndexPrivilege.READ_FAILURE_STORE,
                    resolvePrivilegeAndAssertSingleton(Set.of("indices:data/read/search"))
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.FAILURES)
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(
                Set.of("read_failure_store", "read", "indices:data/read/search", "view_index_metadata")
            );
            assertThat(
                actual,
                containsInAnyOrder(
                    IndexPrivilege.READ_FAILURE_STORE,
                    resolvePrivilegeAndAssertSingleton(Set.of("read", "indices:data/read/search", "view_index_metadata"))
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.FAILURES)
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(
                Set.of("read_failure_store", "all", "read", "indices:data/read/search", "view_index_metadata")
            );
            assertThat(
                actual,
                containsInAnyOrder(
                    resolvePrivilegeAndAssertSingleton(
                        Set.of("read_failure_store", "all", "read", "indices:data/read/search", "view_index_metadata")
                    )
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(actualPredicates, containsInAnyOrder(IndexComponentSelectorPredicate.ALL));
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(
                Set.of("manage", "all", "read", "indices:data/read/search", "view_index_metadata")
            );
            assertThat(
                actual,
                containsInAnyOrder(
                    resolvePrivilegeAndAssertSingleton(Set.of("manage", "all", "read", "indices:data/read/search", "view_index_metadata"))
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(actualPredicates, containsInAnyOrder(IndexComponentSelectorPredicate.ALL));
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(
                Set.of("manage", "read", "indices:data/read/search", "read_failure_store")
            );
            assertThat(
                actual,
                containsInAnyOrder(
                    IndexPrivilege.MANAGE,
                    IndexPrivilege.READ_FAILURE_STORE,
                    resolvePrivilegeAndAssertSingleton(Set.of("read", "indices:data/read/search"))
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(
                    IndexComponentSelectorPredicate.DATA,
                    IndexComponentSelectorPredicate.FAILURES,
                    IndexComponentSelectorPredicate.DATA_AND_FAILURES
                )
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(Set.of("manage", "read", "indices:data/read/search"));
            assertThat(
                actual,
                containsInAnyOrder(IndexPrivilege.MANAGE, resolvePrivilegeAndAssertSingleton(Set.of("read", "indices:data/read/search")))
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.DATA_AND_FAILURES)
            );
        }
        {
            Set<IndexPrivilege> actual = IndexPrivilege.resolveBySelectorAccess(
                Set.of("manage", "read", "manage_data_stream_lifecycle", "indices:admin/*")
            );
            assertThat(
                actual,
                containsInAnyOrder(
                    resolvePrivilegeAndAssertSingleton(Set.of("manage_data_stream_lifecycle", "manage")),
                    resolvePrivilegeAndAssertSingleton(Set.of("read", "indices:admin/*"))
                )
            );
            List<IndexComponentSelectorPredicate> actualPredicates = actual.stream().map(IndexPrivilege::getSelectorPredicate).toList();
            assertThat(
                actualPredicates,
                containsInAnyOrder(IndexComponentSelectorPredicate.DATA, IndexComponentSelectorPredicate.DATA_AND_FAILURES)
            );
        }
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
                resolvePrivilegeAndAssertSingleton(Set.of("view_index_metadata")).automaton,
                resolvePrivilegeAndAssertSingleton(Set.of("manage")).automaton
            ),
            is(true)
        );

        assertThat(
            Automatons.subsetOf(
                resolvePrivilegeAndAssertSingleton(Set.of("monitor")).automaton,
                resolvePrivilegeAndAssertSingleton(Set.of("manage")).automaton
            ),
            is(true)
        );

        assertThat(
            Automatons.subsetOf(
                resolvePrivilegeAndAssertSingleton(Set.of("create", "create_doc", "index", "delete")).automaton,
                resolvePrivilegeAndAssertSingleton(Set.of("write")).automaton
            ),
            is(true)
        );

        assertThat(
            Automatons.subsetOf(
                resolvePrivilegeAndAssertSingleton(Set.of("create_index", "delete_index")).automaton,
                resolvePrivilegeAndAssertSingleton(Set.of("manage")).automaton
            ),
            is(true)
        );
    }

    public void testCrossClusterReplicationPrivileges() {
        final IndexPrivilege crossClusterReplication = resolvePrivilegeAndAssertSingleton(Set.of("cross_cluster_replication"));
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
                IndexPrivilege.resolveBySelectorAccess(Set.of("manage", "read", "monitor"))
                    .stream()
                    .map(p -> p.automaton)
                    .reduce((a1, a2) -> Automatons.unionAndMinimize(List.of(a1, a2)))
                    .get()
            ),
            is(true)
        );

        final IndexPrivilege crossClusterReplicationInternal = resolvePrivilegeAndAssertSingleton(
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
            Automatons.subsetOf(crossClusterReplicationInternal.automaton, resolvePrivilegeAndAssertSingleton(Set.of("manage")).automaton),
            is(false)
        );
        assertThat(
            Automatons.subsetOf(crossClusterReplicationInternal.automaton, resolvePrivilegeAndAssertSingleton(Set.of("all")).automaton),
            is(true)
        );
    }

    public void testInvalidPrivilegeErrorMessage() {
        final String unknownPrivilege = randomValueOtherThanMany(
            i -> IndexPrivilege.values().containsKey(i),
            () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexPrivilege.resolveBySelectorAccess(Set.of(unknownPrivilege))
        );

        final String expectedFullErrorMessage = "unknown index privilege ["
            + unknownPrivilege
            + "]. a privilege must be either "
            + "one of the predefined fixed indices privileges ["
            + Strings.collectionToCommaDelimitedString(IndexPrivilege.names().stream().sorted().collect(Collectors.toList()))
            + "] or a pattern over one of the available index"
            + " actions";

        assertEquals(expectedFullErrorMessage, exception.getMessage());
    }

    public static IndexPrivilege resolvePrivilegeAndAssertSingleton(Set<String> names) {
        final Set<IndexPrivilege> splitBySelector = IndexPrivilege.resolveBySelectorAccess(names);
        assertThat("expected singleton privilege set but got " + splitBySelector, splitBySelector.size(), equalTo(1));
        return splitBySelector.iterator().next();
    }

}
