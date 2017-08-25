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

package org.elasticsearch.client.documentation;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.apache.http.HttpHost;
import org.elasticsearch.cluster.ClusterName;

import java.io.IOException;

/**
 * This class is used to generate the Java Main API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/MainDocumentationIT.java[example]
 * --------------------------------------------------
 */
public class MainDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testMain() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::main-execute
            MainResponse response = client.info();
            //end::main-execute
            assertTrue(response.isAvailable());
            //tag::main-response
            ClusterName clusterName = response.getClusterName(); // <1>
            String clusterUuid = response.getClusterUuid(); // <2>
            String nodeName = response.getNodeName(); // <3>
            Version version = response.getVersion(); // <4>
            Build build = response.getBuild(); // <5>
            //end::main-response
            assertNotNull(clusterName);
            assertNotNull(clusterUuid);
            assertNotNull(nodeName);
            assertNotNull(version);
            assertNotNull(build);
        }
    }

    public void testInitializationFromClientBuilder() throws IOException {
        //tag::rest-high-level-client-init
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
        //end::rest-high-level-client-init

        //tag::rest-high-level-client-close
        client.close();
        //end::rest-high-level-client-close
    }
}
