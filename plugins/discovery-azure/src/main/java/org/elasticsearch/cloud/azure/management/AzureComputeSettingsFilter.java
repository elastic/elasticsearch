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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;

import static org.elasticsearch.cloud.azure.management.AzureComputeService.Management.*;

public class AzureComputeSettingsFilter extends AbstractComponent {

    @Inject
    public AzureComputeSettingsFilter(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        // Cloud management API settings we need to hide
        settingsFilter.addFilter(KEYSTORE_PATH);
        settingsFilter.addFilter(KEYSTORE_PASSWORD);
        settingsFilter.addFilter(KEYSTORE_TYPE);
        settingsFilter.addFilter(SUBSCRIPTION_ID);
    }
}
