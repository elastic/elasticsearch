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

package org.elasticsearch.repositories.azure;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.microsoft.azure.storage.LocationMode;
import com.microsoft.azure.storage.StorageException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ThirdParty;
import org.junit.After;
import org.junit.Before;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.repositories.azure.AzureTestUtils.generateMockSecureSettings;

/**
 * Base class for Azure tests that require credentials.
 * <p>
 * You must specify {@code -Dtests.thirdparty=true -Dtests.azure.account=AzureStorageAccount -Dtests.azure.key=AzureStorageKey}
 * in order to run these tests.
 */
@ThirdParty
public abstract class AbstractAzureWithThirdPartyIntegTestCase extends ESIntegTestCase {

    Settings.Builder generateMockSettings() {
        return Settings.builder().setSecureSettings(generateMockSecureSettings());
    }

    final AzureStorageService azureStorageService = new AzureStorageServiceImpl(generateMockSettings().build(),
        AzureStorageSettings.load(generateMockSettings().build()));
    final String containerName = getContainerName();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return generateMockSettings()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AzureRepositoryPlugin.class);
    }

    public static String getContainerName() {
        /* Have a different name per test so that there is no possible race condition. As the long can be negative,
         * there mustn't be a hyphen between the 2 concatenated numbers
         * (can't have 2 consecutives hyphens on Azure containers)
         */
        String testName = "snapshot-itest-"
            .concat(RandomizedTest.getContext().getRunnerSeedAsString().toLowerCase(Locale.ROOT));
        return testName.contains(" ") ? Strings.split(testName, " ")[0] : testName;
    }

    @Before
    public void createContainer() throws Exception {
        // It could happen that we run this test really close to a previous one
        // so we might need some time to be able to create the container
        assertBusy(() -> {
            try {
                azureStorageService.createContainer("default", LocationMode.PRIMARY_ONLY, getContainerName());
            } catch (URISyntaxException e) {
                // Incorrect URL. This should never happen.
                fail();
            } catch (StorageException e) {
                // It could happen. Let's wait for a while.
                fail();
            }
        }, 30, TimeUnit.SECONDS);
    }

    @After
    public void removeContainer() throws Exception {
        azureStorageService.removeContainer("default", LocationMode.PRIMARY_ONLY, getContainerName());
    }
}
