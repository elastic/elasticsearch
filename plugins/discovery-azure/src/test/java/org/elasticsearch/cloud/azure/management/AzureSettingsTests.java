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

package org.elasticsearch.cloud.azure.management;

import org.elasticsearch.cloud.azure.management.AzureComputeService.Management;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.azure.AzureDiscovery;
import org.elasticsearch.plugin.discovery.azure.AzureDiscoveryPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

public class AzureSettingsTests extends ESTestCase {

    /**
     * We test that with no azure settings but discovery.type not set to azure,
     * the plugin will accept starting.
     */
    public void testEmptySettings() {
        launchPlugin(Settings.builder().build(), null);
    }

    /**
     * We test that with no or incomplete azure settings and discovery.type set to azure,
     * the plugin will refuse starting.
     */
    public void testDiscoveryTypeSetAndIncompleteSettings() {
        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .build(), "one or more azure discovery settings are missing");

        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SUBSCRIPTION_ID, "xxxxx-xxxx-xxxx-xxxxxxxx")
            .build(), "one or more azure discovery settings are missing");

        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SERVICE_NAME, "my-service-name")
            .build(), "one or more azure discovery settings are missing");

        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.KEYSTORE_PATH, "/path/to/my-privatekeystore-file.pkcs12")
            .build(), "one or more azure discovery settings are missing");

        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.KEYSTORE_PASSWORD, "this-is-a-cool-password,-right?")
            .build(), "one or more azure discovery settings are missing");

        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SUBSCRIPTION_ID, "xxxxx-xxxx-xxxx-xxxxxxxx")
            .put(Management.SERVICE_NAME, "my-service-name")
            .put(Management.KEYSTORE_PATH, "/path/to/my-privatekeystore-file.pkcs12")
            .build(), "one or more azure discovery settings are missing");
    }

    /**
     * We test that with all azure settings and discovery.type set to azure,
     * the plugin will start.
     */
    public void testDiscoveryTypeSetAndCompleteSettings() {
        launchPlugin(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SUBSCRIPTION_ID, "xxxxx-xxxx-xxxx-xxxxxxxx")
            .put(Management.SERVICE_NAME, "my-service-name")
            .put(Management.KEYSTORE_PATH, "/path/to/my-privatekeystore-file.pkcs12")
            .put(Management.KEYSTORE_PASSWORD, "this-is-a-cool-password,-right?")
            .build(), null);
    }

    /**
     * Launch Azure Compute Service with:
     * - non existing keystore
     * - wrong keystore password
     * - wrong subscription id
     *
     * In test/resources a sample-privatekeystore-file.pkcs12 keystore file has been created for testing.
     * The associated password is "this-is-a-cool-password,-right?"
     * @throws IOException If can not copy pk store to the tmp dir
     */
    public void testWrongAzureSettings() throws IOException {
        Path keystore = createTempDir().toAbsolutePath().resolve("my-privatekeystore-file.pkcs12");
        launchAzureComputeService(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SUBSCRIPTION_ID, "xxxxx-xxxx-xxxx-xxxxxxxx")
            .put(Management.SERVICE_NAME, "my-service-name")
            .put(Management.KEYSTORE_PATH, keystore)
            .put(Management.KEYSTORE_PASSWORD, "this-is-a-cool-password,-right?")
            .build(), "can not start azure client. Check your elasticsearch configuration.");

        Files.copy(AzureSettingsTests.class.getResourceAsStream("sample-privatekeystore-file.pkcs12"),
            keystore);

        launchAzureComputeService(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SUBSCRIPTION_ID, "xxxxx-xxxx-xxxx-xxxxxxxx")
            .put(Management.SERVICE_NAME, "my-service-name")
            .put(Management.KEYSTORE_PATH, keystore)
            .put(Management.KEYSTORE_PASSWORD, "wrong-password")
            .build(), "can not start azure client. Check your elasticsearch configuration.");

        launchAzureComputeService(Settings.builder()
            .put("discovery.type", AzureDiscovery.AZURE)
            .put(Management.SUBSCRIPTION_ID, "xxxxx-xxxx-xxxx-xxxxxxxx")
            .put(Management.SERVICE_NAME, "my-service-name")
            .put(Management.KEYSTORE_PATH, keystore)
            .put(Management.KEYSTORE_PASSWORD, "this-is-a-cool-password,-right?")
            .build(), "can not start azure client. Check your elasticsearch configuration.");
    }

    private void launchPlugin(Settings settings, String expectedError) {
        AzureDiscoveryPlugin plugin = new AzureDiscoveryPlugin(settings);
        try {
            plugin.nodeModules();
            if (expectedError != null) {
                fail("plugin should not start with wrong settings.");
            }
        } catch (Exception e) {
            if (expectedError == null) {
                logger.error("caught an exception but was not expecting one!", e);
                fail();
            }
            assertThat(e.getMessage(), containsString(expectedError));
        }
    }

    private void launchAzureComputeService(Settings settings, String expectedError) {
        try {
            AzureComputeServiceImpl service = new AzureComputeServiceImpl(settings);
            service.start();
            if (expectedError != null) {
                fail("plugin should not start with wrong settings.");
            }
        } catch (Exception e) {
            if (expectedError == null) {
                logger.error("caught an exception but was not expecting one!", e);
                fail();
            }
            assertThat(e.getMessage(), containsString(expectedError));
        }
    }
}
