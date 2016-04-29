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

import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.junit.listeners.LoggingListener;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.is;

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
 * Backwards compatibility tests are disabled by default via {@link Backwards} annotation.
 * The following system variables control the test execution:
 * <ul>
 *     <li>
 *          <tt>{@value #TESTS_BACKWARDS_COMPATIBILITY}</tt> enables / disables
 *          tests annotated with {@link Backwards} (defaults to
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
@ESBackcompatTestCase.Backwards
@ESIntegTestCase.ClusterScope(minNumDataNodes = 0, maxNumDataNodes = 2, scope = ESIntegTestCase.Scope.SUITE, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class ESBackcompatTestCase extends ESIntegTestCase {

    /**
     * Key used to set the path for the elasticsearch executable used to run backwards compatibility tests from
     * via the commandline -D{@value #TESTS_BACKWARDS_COMPATIBILITY}
     */
    public static final String TESTS_BACKWARDS_COMPATIBILITY = "tests.bwc";
    public static final String TESTS_BACKWARDS_COMPATIBILITY_VERSION = "tests.bwc.version";
    /**
     * Key used to set the path for the elasticsearch executable used to run backwards compatibility tests from
     * via the commandline -D{@value #TESTS_BACKWARDS_COMPATIBILITY_PATH}
     */
    public static final String TESTS_BACKWARDS_COMPATIBILITY_PATH = "tests.bwc.path";
    /**
     * Property that allows to adapt the tests behaviour to older features/bugs based on the input version
     */
    private static final String TESTS_COMPATIBILITY = "tests.compatibility";

    private static final Version GLOBAL_COMPATIBILITY_VERSION = Version.fromString(compatibilityVersionProperty());

    private static Path backwardsCompatibilityPath() {
        String path = System.getProperty(TESTS_BACKWARDS_COMPATIBILITY_PATH);
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Must specify backwards test path with property " + TESTS_BACKWARDS_COMPATIBILITY_PATH);
        }
        String version = System.getProperty(TESTS_BACKWARDS_COMPATIBILITY_VERSION);
        if (version == null || version.isEmpty()) {
            throw new IllegalArgumentException("Must specify backwards test version with property " + TESTS_BACKWARDS_COMPATIBILITY_VERSION);
        }
        if (Version.fromString(version).before(Version.CURRENT.minimumCompatibilityVersion())) {
            throw new IllegalArgumentException("Backcompat elasticsearch version must be same major version as current. " +
                "backcompat: " + version + ", current: " + Version.CURRENT.toString());
        }
        Path file = PathUtils.get(path, "elasticsearch-" + version);
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("Backwards tests location is missing: " + file.toAbsolutePath());
        }
        if (!Files.isDirectory(file)) {
            throw new IllegalArgumentException("Backwards tests location is not a directory: " + file.toAbsolutePath());
        }
        return file;
    }

    /**
     * Retruns the tests compatibility version.
     */
    public Version compatibilityVersion() {
        return compatibilityVersion(getClass());
    }

    private Version compatibilityVersion(Class<?> clazz) {
        if (clazz == Object.class || clazz == ESIntegTestCase.class) {
            return globalCompatibilityVersion();
        }
        CompatibilityVersion annotation = clazz.getAnnotation(CompatibilityVersion.class);
        if (annotation != null) {
            return Version.smallest(Version.fromId(annotation.version()), compatibilityVersion(clazz.getSuperclass()));
        }
        return compatibilityVersion(clazz.getSuperclass());
    }

    /**
     * Returns a global compatibility version that is set via the
     * {@value #TESTS_COMPATIBILITY} or {@value #TESTS_BACKWARDS_COMPATIBILITY_VERSION} system property.
     * If both are unset the current version is used as the global compatibility version. This
     * compatibility version is used for static randomization. For per-suite compatibility version see
     * {@link #compatibilityVersion()}
     */
    public static Version globalCompatibilityVersion() {
        return GLOBAL_COMPATIBILITY_VERSION;
    }

    private static String compatibilityVersionProperty() {
        final String version = System.getProperty(TESTS_COMPATIBILITY);
        if (Strings.hasLength(version)) {
            return version;
        }
        return System.getProperty(TESTS_BACKWARDS_COMPATIBILITY_VERSION);
    }

    public CompositeTestCluster backwardsCluster() {
        return (CompositeTestCluster) cluster();
    }

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        TestCluster cluster = super.buildTestCluster(scope, seed);
        ExternalNode externalNode = new ExternalNode(backwardsCompatibilityPath(), randomLong(), new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return externalNodeSettings(nodeOrdinal);
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return Collections.emptyList();
            }

            @Override
            public Settings transportClientSettings() {
                return transportClientSettings();
            }
        });
        return new CompositeTestCluster((InternalTestCluster) cluster, between(minExternalNodes(), maxExternalNodes()), externalNode);
    }

    private Settings addLoggerSettings(Settings externalNodesSettings) {
        TestLogging logging = getClass().getAnnotation(TestLogging.class);
        Map<String, String> loggingLevels = LoggingListener.getLoggersAndLevelsFromAnnotation(logging);
        Settings.Builder finalSettings = Settings.builder();
        if (loggingLevels != null) {
            for (Map.Entry<String, String> level : loggingLevels.entrySet()) {
                finalSettings.put("logger." + level.getKey(), level.getValue());
            }
        }
        finalSettings.put(externalNodesSettings);
        return finalSettings.build();
    }

    protected int minExternalNodes() { return 1; }

    protected int maxExternalNodes() {
        return 2;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    protected Settings requiredSettings() {
        return ExternalNode.REQUIRED_SETTINGS;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return commonNodeSettings(nodeOrdinal);
    }

    public void assertAllShardsOnNodes(String index, String pattern) {
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndexName())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
    }

    protected Settings commonNodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(requiredSettings());
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, "netty"); // run same transport  / disco as external
        builder.put(Node.NODE_MODE_SETTING.getKey(), "network");
        return builder.build();
    }

    protected Settings externalNodeSettings(int nodeOrdinal) {
        return addLoggerSettings(commonNodeSettings(nodeOrdinal));
    }

    /**
     * Annotation for backwards compat tests
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @TestGroup(enabled = false, sysProperty = ESBackcompatTestCase.TESTS_BACKWARDS_COMPATIBILITY)
    public @interface Backwards {
    }

    /**
     * If a test is annotated with {@link CompatibilityVersion}
     * all randomized settings will only contain settings or mappings which are compatible with the specified version ID.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface CompatibilityVersion {
        int version();
    }
}
