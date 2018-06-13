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

package org.elasticsearch.action.admin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsString;

public class ReloadSecureSettingsIT extends ESIntegTestCase {

    private static final String[] RESOURCE_KEYSTORE_NAMES = new String[] { "elasticsearch_1.keystore", "elasticsearch_2.keystore",
            "elasticsearch_3.keystore", "elasticsearch_4.keystore" };

    public void testMissingKeystoreFile() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
                .stream().findFirst().get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        // keystore file should be missing for this test case
        Files.deleteIfExists(KeyStoreWrapper.keystorePath(environment.configFile()));
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().setSecureStorePassword("").execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            assertThat(nodesReloadResponse, notNullValue());
                            final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                            assertThat(nodesMap.size(), equalTo(cluster().size()));
                            for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                                assertThat(nodeResponse.reloadException(), notNullValue());
                                assertThat(nodeResponse.reloadException(), instanceOf(IllegalStateException.class));
                                assertThat(nodeResponse.reloadException().getMessage(), containsString("Keystore is missing"));
                            }
                        } catch (final AssertionError e) {
                            reloadSettingsError.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                        latch.countDown();
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the missing keystore case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testNullKeystorePassword() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
                .stream().findFirst().get();
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            reloadSettingsError.set(new AssertionError("Null keystore password should fail"));
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            assertThat(e, instanceOf(ActionRequestValidationException.class));
                            assertThat(e.getMessage(), containsString("secure settings password cannot be null"));
                        } catch (final AssertionError ae) {
                            reloadSettingsError.set(ae);
                        } finally {
                            latch.countDown();
                        }
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the null password case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testInvalidKeystoreFile() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
                .stream().findFirst().get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        // invalid "keystore" file should be present in the config dir
        try (InputStream keystore = ReloadSecureSettingsIT.class.getResourceAsStream("invalid.txt.keystore")) {
            if (Files.exists(environment.configFile()) == false) {
                Files.createDirectory(environment.configFile());
            }
            Files.copy(keystore, KeyStoreWrapper.keystorePath(environment.configFile()), StandardCopyOption.REPLACE_EXISTING);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().setSecureStorePassword("").execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            assertThat(nodesReloadResponse, notNullValue());
                            final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                            assertThat(nodesMap.size(), equalTo(cluster().size()));
                            for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                                assertThat(nodeResponse.reloadException(), notNullValue());
                            }
                        } catch (final AssertionError e) {
                            reloadSettingsError.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                        latch.countDown();
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the invalid keystore format case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testWrongKeystorePassword() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
                .stream().findFirst().get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        // "some" keystore should be present in this case
        loadResourceAsKeystore(environment, randomFrom(RESOURCE_KEYSTORE_NAMES));
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().setSecureStorePassword("Wrong password here").execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            assertThat(nodesReloadResponse, notNullValue());
                            final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                            assertThat(nodesMap.size(), equalTo(cluster().size()));
                            for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                                assertThat(nodeResponse.reloadException(), notNullValue());
                                assertThat(nodeResponse.reloadException(), instanceOf(IllegalArgumentException.class));
                                assertThat(nodeResponse.reloadException().getMessage(),
                                        containsString("Keystore format does not accept non-empty passwords"));
                            }
                        } catch (final AssertionError e) {
                            reloadSettingsError.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                        latch.countDown();
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // in the wrong password case no reload should be triggered
        assertThat(mockReloadablePlugin.getReloadCount(), equalTo(initialReloadCount));
    }

    public void testMisbehavingPlugin() throws Exception {
        final Environment environment = internalCluster().getInstance(Environment.class);
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
                .stream().findFirst().get();
        // make plugins throw on reload
        for (final String nodeName : internalCluster().getNodeNames()) {
            internalCluster().getInstance(PluginsService.class, nodeName)
                    .filterPlugins(MisbehavingReloadablePlugin.class)
                    .stream().findFirst().get().setShouldThrow(true);
        }
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        // "some" keystore should be present
        final SecureSettings secureSettings = loadResourceAsKeystore(environment, randomFrom(RESOURCE_KEYSTORE_NAMES));
        // read setting value from the test case (not from the node)
        final String dummyValue = MockReloadablePlugin.DUMMY_SECRET_SETTING
                .get(Settings.builder().put(environment.settings()).setSecureSettings(secureSettings).build())
                .toString();
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().setSecureStorePassword("").execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            assertThat(nodesReloadResponse, notNullValue());
                            final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                            assertThat(nodesMap.size(), equalTo(cluster().size()));
                            for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                                assertThat(nodeResponse.reloadException(), notNullValue());
                                assertThat(nodeResponse.reloadException().getMessage(), containsString("If shouldThrow I throw"));
                            }
                        } catch (final AssertionError e) {
                            reloadSettingsError.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                        latch.countDown();
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
        // even if one plugin fails to reload (throws Exception), others should be
        // unperturbed
        assertThat(mockReloadablePlugin.getReloadCount() - initialReloadCount, equalTo(1));
        // mock plugin should have been reloaded successfully
        assertThat(mockReloadablePlugin.getDummySecretValue(), equalTo(dummyValue));
    }

    public void testReloadWhileKeystoreChanged() throws Exception {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        final MockReloadablePlugin mockReloadablePlugin = pluginsService.filterPlugins(MockReloadablePlugin.class)
                .stream().findFirst().get();
        final Environment environment = internalCluster().getInstance(Environment.class);
        final int initialReloadCount = mockReloadablePlugin.getReloadCount();
        for (int i = 0; i < randomIntBetween(4, 8); i++) {
            // write keystore
            final SecureSettings secureSettings = loadResourceAsKeystore(environment, randomFrom(RESOURCE_KEYSTORE_NAMES));
            // read setting value from the test case (not from the node)
            final String dummyValue = MockReloadablePlugin.DUMMY_SECRET_SETTING
                    .get(Settings.builder().put(environment.settings()).setSecureSettings(secureSettings).build())
                    .toString();
            // reload call
            successfulReloadCall();
            assertThat(mockReloadablePlugin.getDummySecretValue(), equalTo(dummyValue));
            assertThat(mockReloadablePlugin.getReloadCount() - initialReloadCount, equalTo(i + 1));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = Arrays.asList(MockReloadablePlugin.class, MisbehavingReloadablePlugin.class);
        // shuffle as reload is called in order
        Collections.shuffle(plugins);
        return plugins;
    }

    private void successfulReloadCall() throws InterruptedException {
        final AtomicReference<AssertionError> reloadSettingsError = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        client().admin().cluster().prepareReloadSecureSettings().setSecureStorePassword("").execute(
                new ActionListener<NodesReloadSecureSettingsResponse>() {
                    @Override
                    public void onResponse(NodesReloadSecureSettingsResponse nodesReloadResponse) {
                        try {
                            assertThat(nodesReloadResponse, notNullValue());
                            final Map<String, NodesReloadSecureSettingsResponse.NodeResponse> nodesMap = nodesReloadResponse.getNodesMap();
                            assertThat(nodesMap.size(), equalTo(cluster().size()));
                            for (final NodesReloadSecureSettingsResponse.NodeResponse nodeResponse : nodesReloadResponse.getNodes()) {
                                assertThat(nodeResponse.reloadException(), nullValue());
                            }
                        } catch (final AssertionError e) {
                            reloadSettingsError.set(e);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        reloadSettingsError.set(new AssertionError("Nodes request failed", e));
                        latch.countDown();
                    }
                });
        latch.await();
        if (reloadSettingsError.get() != null) {
            throw reloadSettingsError.get();
        }
    }

    private SecureSettings loadResourceAsKeystore(Environment environment, String resourceName)
            throws IOException, GeneralSecurityException {
        try (InputStream keystore = ReloadSecureSettingsIT.class.getResourceAsStream(resourceName)) {
            if (Files.exists(environment.configFile()) == false) {
                Files.createDirectory(environment.configFile());
            }
            Files.copy(keystore, KeyStoreWrapper.keystorePath(environment.configFile()), StandardCopyOption.REPLACE_EXISTING);
        }
        // read the keystore from the testcase code (not node code)
        final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.load(environment.configFile());
        keyStoreWrapper.decrypt(new char[0]);
        return keyStoreWrapper;
    }

    public static class CountingReloadablePlugin extends Plugin implements ReloadablePlugin {

        private volatile int reloadCount;

        public CountingReloadablePlugin() {
        }

        @Override
        public void reload(Settings settings) throws Exception {
            reloadCount++;
        }

        public int getReloadCount() {
            return reloadCount;
        }

    }

    public static class MockReloadablePlugin extends CountingReloadablePlugin {

        static final Setting<SecureString> DUMMY_SECRET_SETTING = SecureSetting.secureString("dummy_secret_setting", null);
        private volatile String dummySecretValue;

        public MockReloadablePlugin() {
        }

        @Override
        public void reload(Settings settings) throws Exception {
            super.reload(settings);
            dummySecretValue = DUMMY_SECRET_SETTING.get(settings).toString();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(DUMMY_SECRET_SETTING);
        }

        public String getDummySecretValue() {
            return dummySecretValue;
        }

    }

    public static class MisbehavingReloadablePlugin extends CountingReloadablePlugin {

        private boolean shouldThrow = false;

        public MisbehavingReloadablePlugin() {
        }

        @Override
        public synchronized void reload(Settings settings) throws Exception {
            super.reload(settings);
            if (shouldThrow) {
                shouldThrow = false;
                throw new Exception("If shouldThrow I throw");
            }
        }

        public synchronized void setShouldThrow(boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }
    }

}
