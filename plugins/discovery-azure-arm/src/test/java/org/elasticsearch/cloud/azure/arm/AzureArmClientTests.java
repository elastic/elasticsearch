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

package org.elasticsearch.cloud.azure.arm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.CLIENT_ID_SETTING;
import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.SECRET_SETTING;
import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.SUBSCRIPTION_ID_SETTING;
import static org.elasticsearch.cloud.azure.arm.AzureManagementService.Management.TENANT_ID_SETTING;

/**
 * This is not really a real test. It's just there to help when we have to write code
 * for this plugin. It helps to make sure that Azure client works as expected with real azure credentials.
 */
public class AzureArmClientTests extends ESTestCase {

    private static final String CLIENT_ID = "FILL_WITH_YOUR_CLIENT_ID";
    private static final String SECRET = "FILL_WITH_YOUR_SECRET";
    private static final String TENANT = "FILL_WITH_YOUR_TENANT";
    private static final String SUBSCRIPTION_ID = "FILL_WITH_YOUR_SUBSCRIPTION_ID";

    public void testConnectWithKeySecret() {
        assumeFalse("Test is skipped unless you use with real credentials",
            CLIENT_ID.startsWith("FILL_WITH_YOUR_") ||
                SECRET.startsWith("FILL_WITH_YOUR_") ||
                TENANT.startsWith("FILL_WITH_YOUR_") ||
                SUBSCRIPTION_ID.startsWith("FILL_WITH_YOUR_"));

        Settings settings = Settings.builder()
            .put(CLIENT_ID_SETTING.getKey(), CLIENT_ID)
            .put(TENANT_ID_SETTING.getKey(), TENANT)
            .put(SECRET_SETTING.getKey(), SECRET)
            .put(SUBSCRIPTION_ID_SETTING.getKey(), SUBSCRIPTION_ID)
            .build();

        AzureManagementServiceImpl service = new AzureManagementServiceImpl(settings);
        List<AzureVirtualMachine> vms = service.getVirtualMachines(null);

        for (AzureVirtualMachine vm : vms) {
            logger.info(" -> {}", vm);
        }
    }
}
