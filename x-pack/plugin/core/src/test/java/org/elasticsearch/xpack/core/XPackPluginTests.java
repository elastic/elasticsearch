/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartBasicResponse;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.license.PutLicenseRequest;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.license.internal.Status;
import org.elasticsearch.license.internal.StatusSupplier;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XPackPluginTests extends ESTestCase {

    private final License mockLicense = mock(License.class);
    private final License.OperationMode operationMode = randomFrom(License.OperationMode.values());

    public void testXPackInstalledAttrClash() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR, randomBoolean());
        if (randomBoolean()) {
            builder.put(Client.CLIENT_TYPE_SETTING_S.getKey(), "transport");
        }
        XPackPlugin xpackPlugin = createXPackPlugin(builder.put("path.home", createTempDir()).build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, xpackPlugin::additionalSettings);
        assertThat(
            e.getMessage(),
            containsString("Directly setting [node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR + "] is not permitted")
        );
    }

    public void testXPackInstalledAttrExists() throws Exception {
        XPackPlugin xpackPlugin = createXPackPlugin(Settings.builder().put("path.home", createTempDir()).build());
        assertEquals("true", xpackPlugin.additionalSettings().get("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR));
    }

    public void testNodesNotReadyForXPackCustomMetadata() {
        boolean compatible;
        boolean nodesCompatible = true;
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();

        for (int i = 0; i < randomInt(3); i++) {
            final Map<String, String> attributes;
            if (randomBoolean()) {
                attributes = Collections.singletonMap(XPackPlugin.XPACK_INSTALLED_NODE_ATTR, "true");
            } else {
                nodesCompatible = false;
                attributes = Collections.emptyMap();
            }

            discoveryNodes.add(
                new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), attributes, Collections.emptySet(), Version.CURRENT)
            );
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterName.DEFAULT);

        if (randomBoolean()) {
            clusterStateBuilder.putCustom(TokenMetadata.TYPE, new TokenMetadata(Collections.emptyList(), new byte[0]));
            compatible = true;
        } else {
            compatible = nodesCompatible;
        }

        ClusterState clusterState = clusterStateBuilder.nodes(discoveryNodes.build()).build();

        assertEquals(XPackPlugin.nodesNotReadyForXPackCustomMetadata(clusterState).isEmpty(), nodesCompatible);
        assertEquals(XPackPlugin.isReadyForXPackCustomMetadata(clusterState), compatible);

        if (compatible == false) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> XPackPlugin.checkReadyForXPackCustomMetadata(clusterState)
            );
            assertThat(e.getMessage(), containsString("The following nodes are not ready yet for enabling x-pack custom metadata:"));
        }
    }

    public void testLoadExtensions() throws Exception {
        XPackPlugin xpackPlugin = createXPackPlugin(Settings.builder().build());
        xpackPlugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                List<Object> extensions = new ArrayList<>();
                if (extensionPointType == MutableLicenseService.class) {
                    extensions.add(new TestLicenseService());
                } else if (extensionPointType == StatusSupplier.class) {
                    extensions.add(new TestStatusSupplier());
                }
                return (List<T>) extensions;
            }
        });
        assertEquals(mockLicense, XPackPlugin.getSharedLicenseService().getLicense());
        assertEquals(operationMode, XPackPlugin.getSharedLicenseState().getOperationMode());
    }

    public void testLoadExtensionsFailure() throws Exception {
        XPackPlugin xpackPlugin = createXPackPlugin(Settings.builder().build());
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> xpackPlugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                    List<Object> extensions = new ArrayList<>();
                    if (extensionPointType == MutableLicenseService.class) {
                        extensions.add(new TestStatusSupplier());
                        extensions.add(new TestStatusSupplier());
                    }
                    return (List<T>) extensions;
                }
            })
        );
        assertThat(
            exception.getMessage(),
            is("interface org.elasticsearch.license.internal.MutableLicenseService " + "may not have multiple implementations")
        );

        IllegalStateException exception2 = expectThrows(
            IllegalStateException.class,
            () -> xpackPlugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
                @Override
                @SuppressWarnings("unchecked")
                public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                    List<Object> extensions = new ArrayList<>();
                    if (extensionPointType == StatusSupplier.class) {
                        extensions.add(new TestLicenseService());
                        extensions.add(new TestLicenseService());
                    }
                    return (List<T>) extensions;
                }
            })
        );
        assertThat(
            exception2.getMessage(),
            is("interface org.elasticsearch.license.internal.StatusSupplier " + "may not have multiple implementations")
        );
    }

    public void testLoadNoExtensions() throws Exception {
        XPackPlugin xpackPlugin = createXPackPlugin(Settings.builder().build());
        xpackPlugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return Collections.emptyList();
            }
        });
        Environment mockEnvironment = mock(Environment.class);
        when(mockEnvironment.settings()).thenReturn(Settings.builder().build());
        when(mockEnvironment.configFile()).thenReturn(PathUtils.get(""));
        xpackPlugin.createComponents(
            null,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            null,
            null,
            null,
            mockEnvironment,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(XPackPlugin.getSharedLicenseService(), instanceOf(ClusterStateLicenseService.class));
        assertEquals(License.OperationMode.TRIAL, XPackPlugin.getSharedLicenseState().getOperationMode());
    }

    private XPackPlugin createXPackPlugin(Settings settings) throws Exception {
        return new XPackPlugin(settings) {

            @Override
            protected void setSslService(SSLService sslService) {
                // disable
            }
        };
    }

    class TestLicenseService implements MutableLicenseService {
        @Override
        public void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener) {}

        @Override
        public void removeLicense(ActionListener<? extends AcknowledgedResponse> listener) {}

        @Override
        public void startBasicLicense(PostStartBasicRequest request, ActionListener<PostStartBasicResponse> listener) {}

        @Override
        public void startTrialLicense(PostStartTrialRequest request, ActionListener<PostStartTrialResponse> listener) {}

        @Override
        public License getLicense() {
            return mockLicense;
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void close() {}
    }

    class TestStatusSupplier implements StatusSupplier {
        @Override
        public Status get() {
            return new Status(operationMode, false, "");
        }
    }

}
