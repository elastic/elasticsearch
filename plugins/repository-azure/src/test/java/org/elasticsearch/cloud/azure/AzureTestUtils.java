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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;

public class AzureTestUtils {
    /**
     * Mock secure settings from sysprops when running integration tests with ThirdParty annotation.
     * Start the tests with {@code -Dtests.azure.account=AzureStorageAccount and -Dtests.azure.key=AzureStorageKey}
     * @return Mock Settings from sysprops
     */
    public static SecureSettings generateMockSecureSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();

        if (Strings.isEmpty(System.getProperty("tests.azure.account")) ||
            Strings.isEmpty(System.getProperty("tests.azure.key"))) {
            throw new IllegalStateException("to run integration tests, you need to set -Dtests.thirdparty=true and " +
                "-Dtests.azure.account=azure-account -Dtests.azure.key=azure-key");
        }

        secureSettings.setString("azure.client.default.account", System.getProperty("tests.azure.account"));
        secureSettings.setString("azure.client.default.key", System.getProperty("tests.azure.key"));

        return secureSettings;
    }
}
