/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CrossClusterEnrichIT extends AbstractEnrichBasedCrossClusterTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return plugins;
    }

    public void testWithHostsPolicy() {
        for (var mode : Enrich.Mode.values()) {
            String query = "FROM events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, null)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(2L, "Android"),
                            List.of(1L, "Linux"),
                            List.of(1L, "MacOS"),
                            List.of(4L, "Windows"),
                            Arrays.asList(1L, (String) null)
                        )
                    )
                );
                assertFalse(resp.getExecutionInfo().isCrossClusterSearch());
            }
        }

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (var mode : Enrich.Mode.values()) {
            String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(1L, "Android"),
                            List.of(2L, "Linux"),
                            List.of(4L, "MacOS"),
                            List.of(3L, "Windows"),
                            List.of(1L, "iOS"),
                            Arrays.asList(2L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }

        for (var mode : Enrich.Mode.values()) {
            String query = "FROM *:events,events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(
                    rows,
                    equalTo(
                        List.of(
                            List.of(3L, "Android"),
                            List.of(3L, "Linux"),
                            List.of(5L, "MacOS"),
                            List.of(7L, "Windows"),
                            List.of(1L, "iOS"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichHostsAggThenEnrichVendorCoordinator() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (var hostMode : Enrich.Mode.values()) {
            String query = String.format(Locale.ROOT, """
                FROM *:events,events
                | eval ip= TO_STR(host)
                | %s
                | stats c = COUNT(*) by os
                | %s
                | stats c = SUM(c) by vendor
                | sort vendor
                """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(3L, "Redhat"),
                            List.of(3L, "Samsung"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichTwiceThenAggs() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (var hostMode : Enrich.Mode.values()) {
            String query = String.format(Locale.ROOT, """
                FROM *:events,events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(3L, "Redhat"),
                            List.of(3L, "Samsung"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichCoordinatorThenAny() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | %s
            | stats c = COUNT(*) by vendor
            | sort vendor
            """, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.ANY));
        try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
            assertThat(
                getValuesList(resp),
                equalTo(
                    List.of(
                        List.of(6L, "Apple"),
                        List.of(7L, "Microsoft"),
                        List.of(3L, "Redhat"),
                        List.of(3L, "Samsung"),
                        Arrays.asList(3L, (String) null)
                    )
                )
            );
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testEnrichCoordinatorWithVendor() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (Enrich.Mode hostMode : Enrich.Mode.values()) {
            String query = String.format(Locale.ROOT, """
                FROM *:events,events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(3L, "Redhat"),
                            List.of(3L, "Samsung"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }

    }

    public void testEnrichRemoteWithVendor() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (Enrich.Mode hostMode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
            var query = String.format(Locale.ROOT, """
                FROM *:events,events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.REMOTE));
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                assertThat(
                    getValuesList(resp),
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(1L, "Redhat"),
                            List.of(2L, "Samsung"),
                            List.of(1L, "Sony"),
                            List.of(2L, "Suse"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testEnrichRemoteWithVendorNoSort() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        for (Enrich.Mode hostMode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
            var query = String.format(Locale.ROOT, """
                FROM *:events,events
                | LIMIT 100
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.REMOTE));
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                var values = getValuesList(resp);
                values.sort(Comparator.comparing(o -> (String) o.get(1), Comparator.nullsLast(Comparator.naturalOrder())));
                assertThat(
                    values,
                    equalTo(
                        List.of(
                            List.of(6L, "Apple"),
                            List.of(7L, "Microsoft"),
                            List.of(1L, "Redhat"),
                            List.of(2L, "Samsung"),
                            List.of(1L, "Sony"),
                            List.of(2L, "Suse"),
                            Arrays.asList(3L, (String) null)
                        )
                    )
                );
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
                assertCCSExecutionInfoDetails(executionInfo);
            }
        }
    }

    public void testTopNThenEnrichRemote() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | SORT timestamp, user, ip
            | LIMIT 5
            | %s | KEEP host, timestamp, user, os
            """, enrichHosts(Enrich.Mode.REMOTE));
        try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
            assertThat(
                getValuesList(resp),
                equalTo(
                    List.of(
                        List.of("192.168.1.2", 1L, "andres", "Windows"),
                        List.of("192.168.1.3", 1L, "matthew", "MacOS"),
                        Arrays.asList("192.168.1.25", 1L, "park", (String) null),
                        List.of("192.168.1.5", 2L, "akio", "Android"),
                        List.of("192.168.1.6", 2L, "sergio", "iOS")
                    )
                )
            );
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testLimitThenEnrichRemote() {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | LIMIT 25
            | eval ip= TO_STR(host)
            | %s | KEEP host, timestamp, user, os
            """, enrichHosts(Enrich.Mode.REMOTE));
        try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
            var values = getValuesList(resp);
            values.sort(
                Comparator.comparingLong((List<Object> o) -> (Long) o.get(1))
                    .thenComparing(o -> (String) o.get(0))
                    .thenComparing(o -> (String) o.get(2))
            );
            assertThat(
                values.subList(0, 5),
                equalTo(
                    List.of(
                        List.of("192.168.1.2", 1L, "andres", "Windows"),
                        Arrays.asList("192.168.1.25", 1L, "park", (String) null),
                        List.of("192.168.1.3", 1L, "matthew", "MacOS"),
                        List.of("192.168.1.5", 2L, "akio", "Android"),
                        List.of("192.168.1.5", 2L, "simon", "Android")
                    )
                )
            );
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", "c1", "c2")));
            assertCCSExecutionInfoDetails(executionInfo);
        }
    }

    public void testAggThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | stats c = COUNT(*) by os
            | %s
            | sort vendor
            """, enrichHosts(Enrich.Mode.ANY), enrichVendors(Enrich.Mode.REMOTE));
        var error = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(error.getMessage(), containsString("ENRICH with remote policy can't be executed after STATS"));
    }

    public void testEnrichCoordinatorThenEnrichRemote() {
        String query = String.format(Locale.ROOT, """
            FROM *:events,events
            | eval ip= TO_STR(host)
            | %s
            | %s
            | sort vendor
            """, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.REMOTE));
        var error = expectThrows(VerificationException.class, () -> runQuery(query, randomBoolean()).close());
        assertThat(
            error.getMessage(),
            containsString("ENRICH with remote policy can't be executed after another ENRICH with coordinator policy")
        );
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo) {
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());
        List<EsqlExecutionInfo.Cluster> clusters = executionInfo.clusterAliases()
            .stream()
            .map(alias -> executionInfo.getCluster(alias))
            .collect(Collectors.toList());

        for (EsqlExecutionInfo.Cluster cluster : clusters) {
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(cluster.getIndexExpression(), equalTo("events"));
            assertThat(cluster.getTotalShards(), equalTo(1));
            assertThat(cluster.getSuccessfulShards(), equalTo(1));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        }
    }
}
