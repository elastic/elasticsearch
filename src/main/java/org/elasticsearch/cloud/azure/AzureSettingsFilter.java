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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.cloud.azure.management.AzureComputeService;
import org.elasticsearch.cloud.azure.storage.AzureStorageService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.SettingsFilter;

/**
 * Filtering cloud.azure.* settings
 */
public class AzureSettingsFilter implements SettingsFilter.Filter {

    @Override
    public void filter(ImmutableSettings.Builder settings) {
        // Cloud global settings
        settings.remove("cloud.azure." + AzureComputeService.Fields.REFRESH);

        // Cloud management API settings
        settings.remove("cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_PATH);
        settings.remove("cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_PASSWORD);
        settings.remove("cloud.azure.management." + AzureComputeService.Fields.KEYSTORE_TYPE);
        settings.remove("cloud.azure.management." + AzureComputeService.Fields.SUBSCRIPTION_ID);
        settings.remove("cloud.azure.management." + AzureComputeService.Fields.SERVICE_NAME);

        // Deprecated Cloud management API settings
        // TODO Remove in 3.0.0
        settings.remove("cloud.azure." + AzureComputeService.Fields.KEYSTORE_DEPRECATED);
        settings.remove("cloud.azure." + AzureComputeService.Fields.PASSWORD_DEPRECATED);
        settings.remove("cloud.azure." + AzureComputeService.Fields.SUBSCRIPTION_ID_DEPRECATED);
        settings.remove("cloud.azure." + AzureComputeService.Fields.SERVICE_NAME_DEPRECATED);

        // Cloud storage API settings
        settings.remove("cloud.azure.storage." + AzureStorageService.Fields.ACCOUNT);
        settings.remove("cloud.azure.storage." + AzureStorageService.Fields.KEY);

        // Deprecated Cloud storage API settings
        // TODO Remove in 3.0.0
        settings.remove("cloud.azure." + AzureStorageService.Fields.ACCOUNT_DEPRECATED);
        settings.remove("cloud.azure." + AzureStorageService.Fields.KEY_DEPRECATED);
    }
}
