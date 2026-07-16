/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfoCluster;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnrichPolicyResolverTests extends ESTestCase {

    private final Map<String, MockTransportService> transports = new HashMap<>();
    private TestThreadPool threadPool;
    private TestEnrichPolicyResolver localCluster;
    private TestEnrichPolicyResolver clusterA;
    private TestEnrichPolicyResolver clusterB;

    @After
    public void stopClusters() {
        transports.values().forEach(TransportService::stop);
        terminate(threadPool);
    }

    @Before
    public void setUpClusters() {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", between(1, 8), 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        for (String cluster : List.of("", "cluster_a", "cluster_b")) {
            var transport = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            );
            transport.acceptIncomingRequests();
            transport.start();
            transports.put(cluster, transport);
        }
        AbstractSimpleTransportTestCase.connectToNode(transports.get(""), transports.get("cluster_a").getLocalNode());
        AbstractSimpleTransportTestCase.connectToNode(transports.get(""), transports.get("cluster_b").getLocalNode());
        final var projectId = randomProjectIdOrDefault();
        localCluster = newEnrichPolicyResolver(projectId, LOCAL_CLUSTER_GROUP_KEY);
        clusterA = newEnrichPolicyResolver(projectId, "cluster_a");
        clusterB = newEnrichPolicyResolver(projectId, "cluster_b");

        // hosts policies are the same across clusters
        var hostsPolicy = new EnrichPolicy("match", null, List.of(), "ip", List.of("region", "cost"));
        var hostsMapping = Map.of("ip", "ip", "region", "keyword", "cost", "long");
        localCluster.aliases.put(".enrich-hosts", ".enrich-hosts-123");
        localCluster.mappings.put(".enrich-hosts-123", hostsMapping);
        localCluster.policies.put("hosts", hostsPolicy);

        clusterA.aliases.put(".enrich-hosts", ".enrich-hosts-999");
        clusterA.mappings.put(".enrich-hosts-999", hostsMapping);
        clusterA.policies.put("hosts", hostsPolicy);

        clusterB.aliases.put(".enrich-hosts", ".enrich-hosts-100");
        clusterB.mappings.put(".enrich-hosts-100", hostsMapping);
        clusterB.policies.put("hosts", hostsPolicy);

        // addresses policies are compatible across clusters
        var addressPolicy = new EnrichPolicy("match", null, List.of(), "emp_id", List.of("country", "city"));
        var addressPolicyA = new EnrichPolicy("match", null, List.of(), "emp_id", List.of("country", "city", "state"));
        var addressPolicyB = new EnrichPolicy("match", null, List.of(), "emp_id", List.of("country", "city"));

        var addressMapping = Map.of("emp_id", "long", "country", "keyword", "city", "keyword");
        var addressMappingA = Map.of("emp_id", "long", "country", "keyword", "city", "keyword", "state", "keyword");
        var addressMappingB = Map.of("emp_id", "long", "country", "keyword", "city", "keyword");

        localCluster.aliases.put(".enrich-address", ".enrich-address-1001");
        localCluster.mappings.put(".enrich-address-1001", addressMapping);
        localCluster.policies.put("address", addressPolicy);

        clusterA.aliases.put(".enrich-address", ".enrich-address-1002");
        clusterA.mappings.put(".enrich-address-1002", addressMappingA);
        clusterA.policies.put("address", addressPolicyA);

        clusterB.aliases.put(".enrich-address", ".enrich-address-1003");
        clusterB.mappings.put(".enrich-address-1003", addressMappingB);
        clusterB.policies.put("address", addressPolicyB);

        // authors are not compatible
        var authorPolicy = new EnrichPolicy("match", null, List.of(), "author", List.of("name", "address"));
        var authorPolicyA = new EnrichPolicy("range", null, List.of(), "author", List.of("name", "address"));
        var authorPolicyB = new EnrichPolicy("match", null, List.of(), "author", List.of("name", "address"));

        var authorMapping = Map.of("author", "keyword", "name", "text", "address", "text");
        var authorMappingA = Map.of("author", "long", "name", "text", "address", "text");
        var authorMappingB = Map.of("author", "long", "name", "text", "address", "text");

        localCluster.aliases.put(".enrich-author", ".enrich-author-X");
        localCluster.mappings.put(".enrich-author-X", authorMapping);
        localCluster.policies.put("author", authorPolicy);

        clusterA.aliases.put(".enrich-author", ".enrich-author-A");
        clusterA.mappings.put(".enrich-author-A", authorMappingA);
        clusterA.policies.put("author", authorPolicyA);

        clusterB.aliases.put(".enrich-author", ".enrich-author-B");
        clusterB.mappings.put(".enrich-author-B", authorMappingB);
        clusterB.policies.put("author", authorPolicyB);
    }

    private void assertHostPolicies(ResolvedEnrichPolicy resolved) {
        assertNotNull(resolved);
        assertThat(resolved.matchField(), equalTo("ip"));
        assertThat(resolved.enrichFields(), equalTo(List.of("region", "cost")));
        assertThat(resolved.mapping().keySet(), containsInAnyOrder("ip", "region", "cost"));
    }

    /**
     * Deterministic per-(policyName, mode) {@link Source} for tests that exercise {@link EnrichPolicyResolver} directly, at
     * the (name, mode)-conflated granularity these tests operate at - real per-subquery scoping is exercised separately by
     * {@code EsqlSessionTests#testComputeEnrichScope}.
     */
    private static Source sourceFor(String policyName, Enrich.Mode mode) {
        return new Source(1, 0, policyName + ":" + mode);
    }

    public void testLocalHosts() {
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            Set<String> clusters = Set.of(LOCAL_CLUSTER_GROUP_KEY);
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("hosts", mode, sourceFor("hosts", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("hosts", mode));
            assertHostPolicies(resolved);
            assertThat(resolved.concreteIndices(), equalTo(Map.of("", ".enrich-hosts-123")));
        }
    }

    public void testRemoteHosts() {
        Set<String> clusters = Set.of("cluster_a", "cluster_b");
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("hosts", mode, sourceFor("hosts", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("hosts", mode));
            assertHostPolicies(resolved);
            var expectedIndices = switch (mode) {
                case COORDINATOR -> Map.of("", ".enrich-hosts-123");
                case ANY -> Map.of("", ".enrich-hosts-123", "cluster_a", ".enrich-hosts-999", "cluster_b", ".enrich-hosts-100");
                case REMOTE -> Map.of("cluster_a", ".enrich-hosts-999", "cluster_b", ".enrich-hosts-100");
            };
            assertThat(resolved.concreteIndices(), equalTo(expectedIndices));
        }
    }

    public void testMixedHosts() {
        Set<String> clusters = Set.of(LOCAL_CLUSTER_GROUP_KEY, "cluster_a", "cluster_b");
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("hosts", mode, sourceFor("hosts", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("hosts", mode));
            assertHostPolicies(resolved);
            var expectedIndices = switch (mode) {
                case COORDINATOR -> Map.of("", ".enrich-hosts-123");
                case ANY, REMOTE -> Map.of("", ".enrich-hosts-123", "cluster_a", ".enrich-hosts-999", "cluster_b", ".enrich-hosts-100");
            };
            assertThat(mode.toString(), resolved.concreteIndices(), equalTo(expectedIndices));
        }
    }

    public void testLocalAddress() {
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            Set<String> clusters = Set.of(LOCAL_CLUSTER_GROUP_KEY);
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("address", mode, sourceFor("address", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("address", mode));
            assertNotNull(resolved);
            assertThat(resolved.matchField(), equalTo("emp_id"));
            assertThat(resolved.enrichFields(), equalTo(List.of("country", "city")));
            assertThat(resolved.mapping().keySet(), containsInAnyOrder("emp_id", "country", "city"));
            assertThat(resolved.concreteIndices(), equalTo(Map.of("", ".enrich-address-1001")));
        }
        {
            List<String> clusters = randomSubsetOf(between(1, 3), List.of("", "cluster_a", "cluster_a"));
            var mode = Enrich.Mode.COORDINATOR;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("address", mode, sourceFor("address", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("address", mode));
            assertNotNull(resolved);
            assertThat(resolved.matchField(), equalTo("emp_id"));
            assertThat(resolved.enrichFields(), equalTo(List.of("country", "city")));
            assertThat(resolved.mapping().keySet(), containsInAnyOrder("emp_id", "country", "city"));
            assertThat(resolved.concreteIndices(), equalTo(Map.of("", ".enrich-address-1001")));
        }
    }

    public void testRemoteAddress() {
        Set<String> clusters = Set.of("cluster_a", "cluster_b");
        for (Enrich.Mode mode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("address", mode, sourceFor("address", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("address", mode)));
            var msg = "enrich policy [address] has different enrich fields across clusters; "
                + "these fields are missing in some policies: [state]";
            assertThat(resolution.getError(sourceFor("address", mode)), equalTo(msg));
        }
    }

    public void testMixedAddress() {
        Set<String> clusters = Set.of(LOCAL_CLUSTER_GROUP_KEY, "cluster_a", "cluster_b");
        for (Enrich.Mode mode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("hosts", mode, sourceFor("hosts", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("hosts", mode));
            assertHostPolicies(resolved);
            assertThat(
                mode.toString(),
                resolved.concreteIndices(),
                equalTo(Map.of("", ".enrich-hosts-123", "cluster_a", ".enrich-hosts-999", "cluster_b", ".enrich-hosts-100"))
            );
        }
    }

    public void testLocalAuthor() {
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            Set<String> clusters = Set.of(LOCAL_CLUSTER_GROUP_KEY);
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("author", mode));
            assertNotNull(resolved);
            assertThat(resolved.matchField(), equalTo("author"));
            assertThat(resolved.enrichFields(), equalTo(List.of("name", "address")));
            assertThat(resolved.mapping().keySet(), containsInAnyOrder("author", "name", "address"));
            assertThat(resolved.concreteIndices(), equalTo(Map.of("", ".enrich-author-X")));
        }
        {
            var mode = Enrich.Mode.COORDINATOR;
            var clusters = randomSubsetOf(between(1, 3), Set.of("", "cluster_a", "cluster_b"));
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("author", mode));
            assertNotNull(resolved);
            assertThat(resolved.matchField(), equalTo("author"));
            assertThat(resolved.matchType(), equalTo("match"));
            assertThat(resolved.enrichFields(), equalTo(List.of("name", "address")));
            assertThat(resolved.mapping().keySet(), containsInAnyOrder("author", "name", "address"));
            assertThat(resolved.concreteIndices(), equalTo(Map.of("", ".enrich-author-X")));
        }
    }

    public void testAuthorClusterA() {
        Set<String> clusters = Set.of("cluster_a");
        {
            var mode = Enrich.Mode.ANY;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("author", mode)));
            assertThat(
                resolution.getError(sourceFor("author", mode)),
                equalTo("enrich policy [author] has different match types [match, range] across clusters")
            );
        }
        {
            var mode = Enrich.Mode.REMOTE;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("author", mode));
            assertNotNull(resolved);
            assertThat(resolved.matchType(), equalTo("range"));
            assertThat(resolved.matchField(), equalTo("author"));
            assertThat(resolved.enrichFields(), equalTo(List.of("name", "address")));
            assertThat(resolved.mapping().keySet(), containsInAnyOrder("author", "name", "address"));
            assertThat(resolved.concreteIndices(), equalTo(Map.of("cluster_a", ".enrich-author-A")));
        }
    }

    public void testAuthorClusterB() {
        Set<String> clusters = Set.of("cluster_b");
        {
            var mode = Enrich.Mode.ANY;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("author", mode)));
            assertThat(
                resolution.getError(sourceFor("author", mode)),
                equalTo("field [author] of enrich policy [author] has different data types [KEYWORD, LONG] across clusters")
            );
        }
        {
            var mode = Enrich.Mode.REMOTE;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            ResolvedEnrichPolicy resolved = resolution.getResolvedPolicy(sourceFor("author", mode));
            assertNotNull(resolved);
            assertThat(resolved.matchType(), equalTo("match"));
            assertThat(resolved.matchField(), equalTo("author"));
            assertThat(resolved.enrichFields(), equalTo(List.of("name", "address")));
            assertThat(resolved.mapping().keySet(), containsInAnyOrder("author", "name", "address"));
            assertThat(resolved.concreteIndices(), equalTo(Map.of("cluster_b", ".enrich-author-B")));
        }
    }

    public void testAuthorClusterAAndClusterB() {
        Set<String> clusters = Set.of("cluster_a", "cluster_b");
        {
            var mode = Enrich.Mode.ANY;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("author", mode)));
            assertThat(
                resolution.getError(sourceFor("author", mode)),
                equalTo("enrich policy [author] has different match types [match, range] across clusters")
            );
        }
        {
            var mode = Enrich.Mode.REMOTE;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("author", mode)));
            assertThat(
                resolution.getError(sourceFor("author", mode)),
                equalTo("enrich policy [author] has different match types [range, match] across clusters")
            );
        }
    }

    public void testLocalAndClusterBAuthor() {
        Set<String> clusters = Set.of("", "cluster_b");
        {
            var mode = Enrich.Mode.ANY;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("author", mode)));
            assertThat(
                resolution.getError(sourceFor("author", mode)),
                equalTo("field [author] of enrich policy [author] has different data types [KEYWORD, LONG] across clusters")
            );
        }
        {
            var mode = Enrich.Mode.REMOTE;
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("author", mode, sourceFor("author", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("author", mode)));
            assertThat(
                resolution.getError(sourceFor("author", mode)),
                equalTo("field [author] of enrich policy [author] has different data types [KEYWORD, LONG] across clusters")
            );
        }
    }

    public void testMissingLocalPolicy() {
        for (Enrich.Mode mode : Enrich.Mode.values()) {
            Set<String> clusters = Set.of("");
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("authoz", mode, sourceFor("authoz", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("authoz", mode)));
            assertThat(
                resolution.getError(sourceFor("authoz", mode)),
                equalTo("cannot find enrich policy [authoz], did you mean [author]?")
            );
        }
    }

    public void testMissingRemotePolicy() {
        {
            var mode = Enrich.Mode.REMOTE;
            Set<String> clusters = Set.of("cluster_a");
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("addrezz", mode, sourceFor("addrezz", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("addrezz", mode)));
            assertThat(
                resolution.getError(sourceFor("addrezz", mode)),
                equalTo("cannot find enrich policy [addrezz] on clusters [cluster_a]")
            );
        }
        {
            var mode = Enrich.Mode.ANY;
            Set<String> clusters = Set.of("cluster_a");
            var resolution = localCluster.resolvePolicies(
                clusters,
                List.of(new EnrichPolicyResolver.UnresolvedPolicy("addrezz", mode, sourceFor("addrezz", mode), new HashSet<>(clusters)))
            );
            assertNull(resolution.getResolvedPolicy(sourceFor("addrezz", mode)));
            assertThat(
                resolution.getError(sourceFor("addrezz", mode)),
                equalTo("cannot find enrich policy [addrezz] on clusters [_local, cluster_a]")
            );
        }
    }

    /**
     * Regression test for a REMOTE-mode ENRICH whose own scope is a single remote cluster that turns out to be unavailable (fails to
     * connect). The correct outcome is the same as if every remote in scope were unavailable: the policy cannot be resolved at all.
     */
    public void testRemoteModeFailsWhenSoleScopedRemoteIsUnavailable() {
        Map<String, EnrichPolicy> policies = new HashMap<>();
        Map<String, String> aliases = new HashMap<>();
        Map<String, Map<String, String>> mappings = new HashMap<>();
        policies.put("custom_policy", new EnrichPolicy("match", null, List.of(), "ip", List.of("region", "cost")));
        aliases.put(".enrich-custom_policy", ".enrich-custom_policy-1");
        mappings.put(".enrich-custom_policy-1", Map.of("ip", "ip", "region", "keyword", "cost", "long"));

        // EnrichPolicyResolver#<init> registers a request handler on its transport service, so this resolver needs its
        // own transport rather than reusing transports.get(LOCAL_CLUSTER_GROUP_KEY) (already claimed by `localCluster`).
        // It's registered under transports so the existing @After hook stops it along with the rest.
        var localTransport = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        localTransport.acceptIncomingRequests();
        localTransport.start();
        transports.put("flaky_local", localTransport);

        // A resolver whose local policies are real (custom_policy exists locally), but whose connection to cluster_a
        // always fails - simulating cluster_a being unavailable regardless of the real (connected) test transport.
        TestEnrichPolicyResolver flakyResolver = new TestEnrichPolicyResolver(
            randomProjectIdOrDefault(),
            "flaky_local",
            policies,
            aliases,
            mappings
        ) {
            @Override
            protected void getRemoteConnection(
                String remoteCluster,
                boolean ensureConnected,
                ActionListener<Transport.Connection> listener
            ) {
                listener.onFailure(new ConnectTransportException(transports.get(remoteCluster).getLocalNode(), "simulated disconnect"));
            }
        };

        var mode = Enrich.Mode.REMOTE;
        // custom_policy's own scope is cluster_a only; LOCAL_CLUSTER_GROUP_KEY is included in the outer `clusters` just
        // to force a real local lookup (so a silently-successful resolution against _local would be observable).
        var resolution = flakyResolver.resolvePolicies(
            Set.of("cluster_a", LOCAL_CLUSTER_GROUP_KEY),
            List.of(new EnrichPolicyResolver.UnresolvedPolicy("custom_policy", mode, sourceFor("custom_policy", mode), Set.of("cluster_a")))
        );
        assertNull(resolution.getResolvedPolicy(sourceFor("custom_policy", mode)));
        assertThat(
            resolution.getError(sourceFor("custom_policy", mode)),
            equalTo("enrich policy [custom_policy] cannot be resolved since remote clusters are unavailable")
        );
    }

    TestEnrichPolicyResolver newEnrichPolicyResolver(ProjectId projectId, String cluster) {
        return new TestEnrichPolicyResolver(projectId, cluster, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    class TestEnrichPolicyResolver extends EnrichPolicyResolver {
        final String cluster;
        final Map<String, EnrichPolicy> policies;
        final Map<String, String> aliases;
        final Map<String, Map<String, String>> mappings;

        TestEnrichPolicyResolver(
            ProjectId projectId,
            String cluster,
            Map<String, EnrichPolicy> policies,
            Map<String, String> aliases,
            Map<String, Map<String, String>> mappings
        ) {
            super(
                mockClusterService(projectId, policies),
                transports.get(cluster),
                new IndexResolver(new FieldCapsClient(threadPool, aliases, mappings), () -> true),
                TestProjectResolvers.singleProject(projectId)
            );
            this.policies = policies;
            this.cluster = cluster;
            this.aliases = aliases;
            this.mappings = mappings;
        }

        EnrichResolution resolvePolicies(Collection<String> clusters, Collection<UnresolvedPolicy> unresolvedPolicies) {
            EsqlExecutionInfo esqlExecutionInfo = createEsqlExecutionInfo(true);
            for (String cluster : clusters) {
                esqlExecutionInfo.swapCluster(cluster, (k, v) -> createEsqlExecutionInfoCluster(cluster, "*"));
            }
            if (randomBoolean()) {
                unresolvedPolicies = new ArrayList<>(unresolvedPolicies);
                Set<String> scope = new HashSet<>(clusters);
                for (Enrich.Mode mode : Enrich.Mode.values()) {
                    for (String policy : List.of("hosts", "address", "author")) {
                        if (randomBoolean()) {
                            unresolvedPolicies.add(new UnresolvedPolicy(policy, mode, sourceFor(policy, mode), scope));
                        }
                    }
                }
                if (randomBoolean()) {
                    Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                    unresolvedPolicies.add(new UnresolvedPolicy("legacy-policy-1", mode, sourceFor("legacy-policy-1", mode), scope));
                }
            }
            PlainActionFuture<EnrichResolution> future = new PlainActionFuture<>();
            super.doResolvePolicies(new HashSet<>(clusters), unresolvedPolicies, esqlExecutionInfo, TransportVersion.current(), future);
            return future.actionGet(30, TimeUnit.SECONDS);
        }

        @Override
        protected void getRemoteConnection(String remoteCluster, boolean ensureConnected, ActionListener<Transport.Connection> listener) {
            assertThat("Must only called on the local cluster", cluster, equalTo(LOCAL_CLUSTER_GROUP_KEY));
            listener.onResponse(transports.get("").getConnection(transports.get(remoteCluster).getLocalNode()));
        }

        static ClusterService mockClusterService(ProjectId projectId, Map<String, EnrichPolicy> policies) {
            ClusterService clusterService = mock(ClusterService.class);
            EnrichMetadata enrichMetadata = new EnrichMetadata(policies);
            ClusterState state = ClusterState.builder(new ClusterName("test"))
                .putProjectMetadata(ProjectMetadata.builder(projectId).customs(Map.of(EnrichMetadata.TYPE, enrichMetadata)))
                .build();
            when(clusterService.state()).thenReturn(state);
            return clusterService;
        }
    }

    static class FieldCapsClient extends FilterClient {
        final Map<String, String> aliases;
        final Map<String, Map<String, String>> mappings;

        FieldCapsClient(ThreadPool threadPool, Map<String, String> aliases, Map<String, Map<String, String>> mappings) {
            super(new NoOpClient(threadPool));
            this.aliases = aliases;
            this.mappings = mappings;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request transportRequest,
            ActionListener<Response> listener
        ) {
            assertThat(transportRequest, instanceOf(FieldCapabilitiesRequest.class));
            FieldCapabilitiesRequest r = (FieldCapabilitiesRequest) transportRequest;
            assertThat(r.indices(), arrayWithSize(1));
            String alias = aliases.get(r.indices()[0]);
            assertNotNull(alias);
            Map<String, String> mapping = mappings.get(alias);
            final FieldCapabilitiesResponse response;
            if (mapping != null) {
                Map<String, IndexFieldCapabilities> fieldCaps = new HashMap<>();
                for (Map.Entry<String, String> e : mapping.entrySet()) {
                    var f = new IndexFieldCapabilitiesBuilder(e.getKey(), e.getValue()).isSearchable(false).isAggregatable(false).build();
                    fieldCaps.put(e.getKey(), f);
                }
                var indexResponse = new FieldCapabilitiesIndexResponse(alias, null, fieldCaps, true, IndexMode.STANDARD);
                response = FieldCapabilitiesResponse.builder().withIndexResponses(List.of(indexResponse)).build();
            } else {
                response = FieldCapabilitiesResponse.empty();
            }
            threadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)
                .execute(ActionRunnable.supply(listener, () -> (Response) new EsqlResolveFieldsResponse(response)));
        }
    }
}
