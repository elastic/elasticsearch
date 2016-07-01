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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugin.repository.azure.AzureRepositoryPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * Azure Repository
 * Main class to easily run Azure from a IDE.
 * It sets all the options to run the Azure plugin and access it from Sense.
 *
 * In order to run this class set configure the following:
 * 1) Set `-Des.path.home=` to a directory containing an ES config directory
 * 2) Set `-Dcloud.azure.storage.my_account.account=account_name`
 * 3) Set `-Dcloud.azure.storage.my_account.key=account_key`
 *
 * Then you can run REST calls like:
 * <pre>
 # Clean test env
 curl -XDELETE localhost:9200/foo?pretty
 curl -XDELETE localhost:9200/_snapshot/my_backup1?pretty
 curl -XDELETE localhost:9200/_snapshot/my_backup2?pretty

 # Create data
 curl -XPUT localhost:9200/foo/bar/1?pretty -d '{
 "foo": "bar"
 }'
 curl -XPOST localhost:9200/foo/_refresh?pretty
 curl -XGET localhost:9200/foo/_count?pretty

 # Create repository using default account
 curl -XPUT localhost:9200/_snapshot/my_backup1?pretty -d '{
   "type": "azure"
 }'

 # Backup
 curl -XPOST "localhost:9200/_snapshot/my_backup1/snap1?pretty&amp;wait_for_completion=true"

 # Remove data
 curl -XDELETE localhost:9200/foo?pretty

 # Restore data
 curl -XPOST "localhost:9200/_snapshot/my_backup1/snap1/_restore?pretty&amp;wait_for_completion=true"
 curl -XGET localhost:9200/foo/_count?pretty
 </pre>
 *
 * If you want to define a secondary repository:
 *
 * 4) Set `-Dcloud.azure.storage.my_account.default=true`
 * 5) Set `-Dcloud.azure.storage.my_account2.account=account_name`
 * 6) Set `-Dcloud.azure.storage.my_account2.key=account_key_secondary`
 *
 * Then you can run REST calls like:
 * <pre>
 # Remove data
 curl -XDELETE localhost:9200/foo?pretty

 # Create repository using account2 (secondary)
 curl -XPUT localhost:9200/_snapshot/my_backup2?pretty -d '{
   "type": "azure",
   "settings": {
     "account" : "my_account2",
     "location_mode": "secondary_only"
   }
 }'

 # Restore data from the secondary endpoint
 curl -XPOST "localhost:9200/_snapshot/my_backup2/snap1/_restore?pretty&amp;wait_for_completion=true"
 curl -XGET localhost:9200/foo/_count?pretty
 </pre>
 */
public class AzureRepositoryF {
    public static void main(String[] args) throws Throwable {
        Settings.Builder settings = Settings.builder();
        settings.put("http.cors.enabled", "true");
        settings.put("http.cors.allow-origin", "*");
        settings.put("cluster.name", AzureRepositoryF.class.getSimpleName());

        // Example for azure repo settings
        // settings.put("cloud.azure.storage.my_account1.account", "account_name");
        // settings.put("cloud.azure.storage.my_account1.key", "account_key");
        // settings.put("cloud.azure.storage.my_account1.default", true);
        // settings.put("cloud.azure.storage.my_account2.account", "account_name");
        // settings.put("cloud.azure.storage.my_account2.key", "account_key_secondary");

        final CountDownLatch latch = new CountDownLatch(1);
        final Node node = new MockNode(settings.build(), Collections.singletonList(AzureRepositoryPlugin.class));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    IOUtils.close(node);
                } catch (IOException e) {
                    throw new ElasticsearchException(e);
                } finally {
                    latch.countDown();
                }
            }
        });
        node.start();
        latch.await();
    }
}
