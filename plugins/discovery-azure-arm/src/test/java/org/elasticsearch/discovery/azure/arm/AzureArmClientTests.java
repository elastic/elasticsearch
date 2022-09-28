/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.azure.arm;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;

import static org.hamcrest.Matchers.not;

public class AzureArmClientTests extends ESTestCase {

    private static final String CLIENT_ID = "FILL_WITH_YOUR_CLIENT_ID";
    private static final String SECRET = "FILL_WITH_YOUR_SECRET";
    private static final String TENANT = "FILL_WITH_YOUR_TENANT";
    private static final String SUBSCRIPTION_ID = "FILL_WITH_YOUR_SUBSCRIPTION_ID";
    private static final String GROUP_NAME = null;

    private static AzureManagementService service;

    @BeforeClass
    public static void createAzureClient() {
        assumeFalse(
            "Test is skipped unless you use with real credentials",
            CLIENT_ID.startsWith("FILL_WITH_YOUR_")
                || SECRET.startsWith("FILL_WITH_YOUR_")
                || TENANT.startsWith("FILL_WITH_YOUR_")
                || SUBSCRIPTION_ID.startsWith("FILL_WITH_YOUR_")
        );

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureClientSettings.CLIENT_ID_SETTING.getKey(), CLIENT_ID);
        secureSettings.setString(AzureClientSettings.TENANT_ID_SETTING.getKey(), TENANT);
        secureSettings.setString(AzureClientSettings.SECRET_SETTING.getKey(), SECRET);
        secureSettings.setString(AzureClientSettings.SUBSCRIPTION_ID_SETTING.getKey(), SUBSCRIPTION_ID);
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();

        service = new AzureManagementService(settings);
    }

    @AfterClass
    public static void waitForHttpClientToClose() {
        service.close();
    }

    public void testConnectWithKeySecret() {
        List<AzureVirtualMachine> vms = service.getVirtualMachines(GROUP_NAME);

        assumeFalse("We continue testing only if there are some existing VMs", vms.isEmpty());
        for (AzureVirtualMachine vm : vms) {
            logger.info(" -> {}", vm);
            assertThat(vm.name(), not(Matchers.isEmptyOrNullString()));
        }
    }
}
