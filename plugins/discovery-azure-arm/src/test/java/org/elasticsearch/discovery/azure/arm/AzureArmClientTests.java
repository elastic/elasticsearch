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

package org.elasticsearch.discovery.azure.arm;

import okhttp3.OkHttpClient;
import okio.AsyncTimeout;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;

import static org.hamcrest.Matchers.not;

/**
 * This is not really a real test. It's just there to help when we have to write code
 * for this plugin. It helps to make sure that Azure client works as expected with real azure credentials.
 */
public class AzureArmClientTests extends ESTestCase {

    private static final String CLIENT_ID = "FILL_WITH_YOUR_CLIENT_ID";
    private static final String SECRET = "FILL_WITH_YOUR_SECRET";
    private static final String TENANT = "FILL_WITH_YOUR_TENANT";
    private static final String SUBSCRIPTION_ID = "FILL_WITH_YOUR_SUBSCRIPTION_ID";
    private static final String GROUP_NAME = null;

    private static AzureManagementServiceImpl service;

    @BeforeClass
    public static void createAzureClient() {
        assumeFalse("Test is skipped unless you use with real credentials",
            CLIENT_ID.startsWith("FILL_WITH_YOUR_") ||
                SECRET.startsWith("FILL_WITH_YOUR_") ||
                TENANT.startsWith("FILL_WITH_YOUR_") ||
                SUBSCRIPTION_ID.startsWith("FILL_WITH_YOUR_"));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureClientSettings.CLIENT_ID_SETTING.getKey(), CLIENT_ID);
        secureSettings.setString(AzureClientSettings.TENANT_ID_SETTING.getKey(), TENANT);
        secureSettings.setString(AzureClientSettings.SECRET_SETTING.getKey(), SECRET);
        secureSettings.setString(AzureClientSettings.SUBSCRIPTION_ID_SETTING.getKey(), SUBSCRIPTION_ID);
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();

        service = new AzureManagementServiceImpl(settings);
    }

    public void testConnectWithKeySecret() {
        List<AzureVirtualMachine> vms = service.getVirtualMachines(GROUP_NAME);

        assumeFalse("We continue testing only if there are some existing VMs", vms.isEmpty());
        for (AzureVirtualMachine vm : vms) {
            logger.info(" -> {}", vm);
            assertThat(vm.getName(), not(Matchers.isEmptyOrNullString()));
        }
    }

    /**
     * This is super ugly. The HTTP client which is used behind the scene
     * by the azure client does not close its resources.
     * The only workaround for now is to wait for 60s so the client
     * will shutdown "normally".
     * See discussion on https://github.com/Azure/azure-sdk-for-java/issues/1387
     */
    @AfterClass
    public static void waitForHttpClientToClose() throws Exception {
        if (service != null) {
            service.close();
            OkHttpClient okHttpClient = service.restClient.httpClient();
            okHttpClient.dispatcher().executorService().shutdown();
            okHttpClient.connectionPool().evictAll();
            synchronized (okHttpClient.connectionPool()) {
                okHttpClient.connectionPool().notifyAll();
            }
            synchronized (AsyncTimeout.class) {
                AsyncTimeout.class.notifyAll();
            }

            Thread.sleep(60000);
        }

        service = null;
    }
}
