/*
 * Licensed to ElasticSearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Test;

import java.io.File;

/**
 * To run this *test*, you need first to modify test/resources dir:
 * - elasticsearch.yml: set your azure password and azure subscription_id
 * - azure.crt: fill this file with your azure certificate (PEM format)
 * - azure.pk: fill this file with your azure private key (PEM format)
 */
public class AzureSimpleTest {

    @Test
    public void launchNode() {
        File dataDir = new File("./target/es/data");
        if(dataDir.exists()) {
            FileSystemUtils.deleteRecursively(dataDir, true);
        }

        // Then we start our node for tests
        Node node = NodeBuilder
                .nodeBuilder()
                .settings(
                        ImmutableSettings.settingsBuilder()
                                .put("gateway.type", "local")
                                .put("path.data", "./target/es/data")
                                .put("path.logs", "./target/es/logs")
                                .put("path.work", "./target/es/work")
                ).node();

        // We wait now for the yellow (or green) status
//        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

    }
}
