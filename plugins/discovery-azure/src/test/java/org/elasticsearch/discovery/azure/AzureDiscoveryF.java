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

package org.elasticsearch.discovery.azure;

import org.elasticsearch.Version;
import org.elasticsearch.cloud.azure.management.AzureComputeService.Management;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.discovery.azure.AzureDiscoveryPlugin;

import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

/**
 * This class is a runner to test manually azure on demand.
 * You need to launch it with the following parameters:
 * - Azure subscription ID
 * - Azure service name (without .cloudapp.net)
 * - /path/to/azure-keystore.pkcs12
 * - keystore-password (if not set, will be asked)
 */
public class AzureDiscoveryF {
    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("cluster.name", AzureDiscoveryF.class.getSimpleName());
        settings.put("path.home", PathUtils.get(System.getProperty("java.io.tmpdir")));
        settings.put("discovery.type", AzureDiscovery.AZURE);

        if (args.length < 3) {
            System.err.println("Wrong number of arguments. 3 required.\n" +
                "Please provide azure_subscription_id service_name path/to/keystore.pkcs12 [password]");
        } else {
            settings.put(Management.SUBSCRIPTION_ID, args[0]);
            settings.put(Management.SERVICE_NAME, args[1]);
            settings.put(Management.KEYSTORE_PATH, args[2]);

            String password;

            if (args.length > 3) {
                password = args[3];
            } else {
                System.out.println("Enter your keystore password");
                Scanner reader = new Scanner(System.in, "UTF-8");
                password = reader.next();
            }

            settings.put(Management.KEYSTORE_PASSWORD, password);

            final CountDownLatch latch = new CountDownLatch(1);
            final Node node = new MockNode(settings.build(), Version.CURRENT, Collections.singletonList(AzureDiscoveryPlugin.class));
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    node.close();
                    latch.countDown();
                }
            });
            node.start();
            latch.await();
        }
    }
}
