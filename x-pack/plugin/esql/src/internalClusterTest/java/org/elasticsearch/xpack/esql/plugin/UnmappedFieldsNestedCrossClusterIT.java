/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractCrossClusterTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.Doc;
import org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.Expectation;
import org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.Kind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.assumeSupported;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.createIndex;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.indexDocs;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.intercept;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.loadAllExpectations;
import static org.elasticsearch.xpack.esql.plugin.UnmappedFieldsNestedFixture.loadExpectations;
import static org.hamcrest.Matchers.equalTo;

/**
 * Puts the index where {@code nested_punk.subfield} is nested and the one where it is unmapped in different clusters, and forces either
 * cluster to evaluate first.
 */
public class UnmappedFieldsNestedCrossClusterIT extends AbstractCrossClusterTestCase {

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false);
    }

    public void testLoadNestedLocallyMissingRemotely() {
        assertAcrossClusters(false, true);
    }

    public void testLoadNestedRemotelyMissingLocally() {
        assertAcrossClusters(false, false);
    }

    public void testLoadAllNestedLocallyMissingRemotely() {
        assertAcrossClusters(true, true);
    }

    public void testLoadAllNestedRemotelyMissingLocally() {
        assertAcrossClusters(true, false);
    }

    /**
     * Queries with and without an object-mapped index, which turns the path from unmapped to partially mapped for the coordinator.
     */
    private void assertAcrossClusters(boolean loadAll, boolean nestedLocally) {
        assumeSupported(loadAll);
        String localNode = randomDataNode(LOCAL_CLUSTER);
        String remoteNode = randomDataNode(REMOTE_CLUSTER_1);
        Map<String, Kind> kinds = new LinkedHashMap<>();
        List<Doc> docs = new ArrayList<>();
        for (Kind kind : Kind.values()) {
            boolean local = switch (kind) {
                case NESTED -> nestedLocally;
                case MISSING -> nestedLocally == false;
                case OBJECT -> randomBoolean();
            };
            String cluster = local ? LOCAL_CLUSTER : REMOTE_CLUSTER_1;
            String index = kind.name().toLowerCase(Locale.ROOT) + "_" + randomIdentifier();
            createIndex(client(cluster), kind, index, local ? localNode : remoteNode);
            docs.addAll(indexDocs(client(cluster), kind, index, between(2, 6)));
            kinds.put(local ? index : REMOTE_CLUSTER_1 + ":" + index, kind);
        }
        for (boolean withObject : new boolean[] { false, true }) {
            List<String> indices = kinds.keySet().stream().filter(index -> withObject || kinds.get(index) != Kind.OBJECT).toList();
            List<Doc> scoped = docs.stream().filter(d -> withObject || d.kind() != Kind.OBJECT).toList();
            String from = "FROM " + String.join(", ", indices);
            for (Expectation expectation : loadAll ? loadAllExpectations(from, scoped) : loadExpectations(from, scoped)) {
                for (boolean localFirst : new boolean[] { true, false }) {
                    assertScheduled(localNode, localFirst, expectation);
                }
            }
        }
    }

    private void assertScheduled(String localNode, boolean localFirst, Expectation expectation) {
        AtomicInteger localHandled = new AtomicInteger();
        AtomicInteger remoteHandled = new AtomicInteger();
        String schedule = localFirst ? "[local cluster first]" : "[remote cluster first]";
        try {
            schedule(localNode, localFirst, localHandled, remoteHandled);
            try (EsqlQueryResponse response = runQuery(expectation.query(), randomBoolean())) {
                expectation.assertMatches(schedule, response);
            }
            assertThat(
                schedule + " not applied to " + expectation.query(),
                List.of(localHandled.get(), remoteHandled.get()),
                equalTo(List.of(1, 1))
            );
        } finally {
            localTransport(localNode).clearInboundRules();
            remoteTransports().forEach(MockTransportService::clearInboundRules);
        }
    }

    /**
     * Orders the local data node's request against the remote cluster's, see {@link UnmappedFieldsNestedFixture#intercept}.
     */
    private void schedule(String localNode, boolean localFirst, AtomicInteger localHandled, AtomicInteger remoteHandled) {
        SubscribableListener<Void> firstDone = new SubscribableListener<>();
        intercept(
            localTransport(localNode),
            ComputeService.DATA_ACTION_NAME,
            localFirst ? null : firstDone,
            localFirst ? firstDone : null,
            counting(localHandled)
        );
        for (MockTransportService remote : remoteTransports()) {
            intercept(
                remote,
                ComputeService.CLUSTER_ACTION_NAME,
                localFirst ? firstDone : null,
                localFirst ? null : firstDone,
                counting(remoteHandled)
            );
        }
    }

    private static UnaryOperator<TransportRequest> counting(AtomicInteger handled) {
        return request -> {
            handled.incrementAndGet();
            return request;
        };
    }

    private MockTransportService localTransport(String node) {
        return asInstanceOf(MockTransportService.class, cluster(LOCAL_CLUSTER).getInstance(TransportService.class, node));
    }

    private List<MockTransportService> remoteTransports() {
        List<MockTransportService> transports = new ArrayList<>();
        for (TransportService transportService : cluster(REMOTE_CLUSTER_1).getInstances(TransportService.class)) {
            transports.add(asInstanceOf(MockTransportService.class, transportService));
        }
        return transports;
    }

    private String randomDataNode(String clusterAlias) {
        return randomFrom(cluster(clusterAlias).clusterService().state().nodes().getDataNodes().values()).getName();
    }
}
