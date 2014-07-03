/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty.NettyTransportModule;
import org.junit.Before;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;

/**
 * Abstract base class for backwards compatibility tests. Subclasses of this class
 * can run tests against a mixed version cluster. A subset of the nodes in the cluster
 * are started in dedicated process running off a full fledged elasticsearch release.
 * Nodes can be "upgraded" from the "backwards" node to an "new" node where "new" nodes
 * version corresponds to current version.
 * The purpose of this test class is to run tests in scenarios where clusters are in an
 * intermediate state during a rolling upgrade as well as upgrade situations. The clients
 * accessed via #client() are random clients to the nodes in the cluster which might
 * execute requests on the "new" as well as the "old" nodes.
 * <p>
 *   Note: this base class is still experimental and might have bugs or leave external processes running behind.
 * </p>
 * Backwards compatibility tests are disabled by default via {@link org.apache.lucene.util.AbstractRandomizedTest.Backwards} annotation.
 * The following system variables control the test execution:
 * <ul>
 *     <li>
 *          <tt>{@value #TESTS_BACKWARDS_COMPATIBILITY}</tt> enables / disables
 *          tests annotated with {@link org.apache.lucene.util.AbstractRandomizedTest.Backwards} (defaults to
 *          <tt>false</tt>)
 *     </li>
 *     <li>
 *          <tt>{@value #TESTS_BACKWARDS_COMPATIBILITY_VERSION}</tt>
 *          sets the version to run the external nodes from formatted as <i>X.Y.Z</i>.
 *          The tests class will try to locate a release folder <i>elasticsearch-X.Y.Z</i>
 *          within path passed via {@value #TESTS_BACKWARDS_COMPATIBILITY_PATH}
 *          depending on this system variable.
 *     </li>
 *     <li>
 *          <tt>{@value #TESTS_BACKWARDS_COMPATIBILITY_PATH}</tt> the path to the
 *          elasticsearch releases to run backwards compatibility tests against.
 *     </li>
 * </ul>
 *
 */
// the transportClientRatio is tricky here since we don't fully control the cluster nodes
@AbstractRandomizedTest.Backwards
@ElasticsearchIntegrationTest.ClusterScope(minNumDataNodes = 0, maxNumDataNodes = 2, scope = ElasticsearchIntegrationTest.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0.0)
@Ignore
public abstract class ElasticsearchBackwardsCompatIntegrationTest extends ElasticsearchIntegrationTest {

    private static File backwardsCompatibilityPath() {
        String path = System.getProperty(TESTS_BACKWARDS_COMPATIBILITY_PATH);
        String version = System.getProperty(TESTS_BACKWARDS_COMPATIBILITY_VERSION);
        if (path == null || path.isEmpty() || version == null || version.isEmpty()) {
            throw new IllegalArgumentException("Invalid Backwards tests location path:" + path + " version: " + version);
        }
        File file = new File(path, "elasticsearch-" + version);
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("Backwards tests location is not a directory: " + file.getAbsolutePath());
        }
        return file;
    }

    public CompositeTestCluster backwardsCluster() {
        return (CompositeTestCluster) cluster();
    }

    protected TestCluster buildTestCluster(Scope scope) throws IOException {
        TestCluster cluster = super.buildTestCluster(scope);
        return new CompositeTestCluster((InternalTestCluster) cluster, between(minExternalNodes(), maxExternalNodes()), new ExternalNode(backwardsCompatibilityPath(), randomLong()));
    }

    protected int minExternalNodes() {
        return 1;
    }

    protected int maxExternalNodes() {
        return 2;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    @Before
    public final void beforeTest() {
        // 1.0.3 is too flaky - lets get stable first.
        assumeTrue("BWC tests are disabled currently for version [< 1.1.0]", compatibilityVersion().onOrAfter(Version.V_1_1_0));
    }

    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(TransportModule.TRANSPORT_TYPE_KEY, NettyTransportModule.class) // run same transport  / disco as external
                .put(DiscoveryModule.DISCOVERY_TYPE_KEY, ZenDiscoveryModule.class)
                .put("node.mode", "network") // we need network mode for this
                .put("gateway.type", "local") // we require local gateway to mimic upgrades of nodes
                .put("discovery.type", "zen") // zen is needed since we start external nodes
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, TransportService.class.getName())
                .build();
    }
}
