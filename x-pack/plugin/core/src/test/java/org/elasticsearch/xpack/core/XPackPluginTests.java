/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.ClusterStateLicenseService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartBasicResponse;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.license.PutLicenseRequest;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XPackPluginTests extends ESTestCase {

    private License license;
    private License.LicenseType licenseType;

    @Before
    public void setup() {
        licenseType = randomFrom(License.LicenseType.values());
        License.Builder builder = License.builder()
            .uid(UUIDs.randomBase64UUID(random()))
            .type(licenseType)
            .issueDate(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(100))
            .expiryDate(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(100))
            .issuedTo("me")
            .issuer("you");
        if (licenseType.equals(License.LicenseType.ENTERPRISE)) {
            builder.maxResourceUnits(1);
        } else {
            builder.maxNodes(1);
        }
        license = builder.build();
    }

    public void testXPackInstalledAttrClash() throws Exception {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr." + XPackPlugin.XPACK_INSTALLED_NODE_ATTR, randomBoolean());
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

            discoveryNodes.add(DiscoveryNodeUtils.create("node_" + i, buildNewFakeTransportAddress(), attributes, Collections.emptySet()));
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
                }
                return (List<T>) extensions;
            }
        });

        Environment mockEnvironment = mock(Environment.class);
        when(mockEnvironment.settings()).thenReturn(Settings.builder().build());
        when(mockEnvironment.configDir()).thenReturn(PathUtils.get(""));
        // ensure createComponents does not influence the results
        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.clusterService()).thenReturn(mock(ClusterService.class));
        when(services.threadPool()).thenReturn(mock(ThreadPool.class));
        when(services.environment()).thenReturn(mockEnvironment);
        xpackPlugin.createComponents(services);
        assertEquals(license, XPackPlugin.getSharedLicenseService().getLicense());
        assertEquals(License.OperationMode.resolve(licenseType), XPackPlugin.getSharedLicenseState().getOperationMode());
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
                        extensions.add(new TestLicenseService());
                        extensions.add(new TestLicenseService());
                    }
                    return (List<T>) extensions;
                }
            })
        );
        assertThat(
            exception.getMessage(),
            is("interface org.elasticsearch.license.internal.MutableLicenseService " + "may not have multiple implementations")
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
        when(mockEnvironment.configDir()).thenReturn(PathUtils.get(""));
        Plugin.PluginServices services = mock(Plugin.PluginServices.class);
        when(services.clusterService()).thenReturn(mock(ClusterService.class));
        when(services.threadPool()).thenReturn(mock(ThreadPool.class));
        when(services.environment()).thenReturn(mockEnvironment);
        xpackPlugin.createComponents(services);
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
        public void removeLicense(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            ActionListener<? extends AcknowledgedResponse> listener
        ) {}

        @Override
        public void startBasicLicense(PostStartBasicRequest request, ActionListener<PostStartBasicResponse> listener) {}

        @Override
        public void startTrialLicense(PostStartTrialRequest request, ActionListener<PostStartTrialResponse> listener) {}

        @Override
        public License getLicense() {
            return license;
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
}
