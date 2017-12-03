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

package org.elasticsearch.client.sniff.documentation;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.ElasticsearchHostsSniffer;
import org.elasticsearch.client.sniff.HostsSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to generate the Java low-level REST client documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/SnifferDocumentation.java[example]
 * --------------------------------------------------
 *
 * Note that this is not a test class as we are only interested in testing that docs snippets compile. We don't want
 * to send requests to a node and we don't even have the tools to do it.
 */
@SuppressWarnings("unused")
public class SnifferDocumentation {

    @SuppressWarnings("unused")
    public void testUsage() throws IOException {
        {
            //tag::sniffer-init
            RestClient restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"))
                    .build();
            Sniffer sniffer = Sniffer.builder(restClient).build();
            //end::sniffer-init

            //tag::sniffer-close
            sniffer.close();
            restClient.close();
            //end::sniffer-close
        }
        {
            //tag::sniffer-interval
            RestClient restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"))
                    .build();
            Sniffer sniffer = Sniffer.builder(restClient)
                    .setSniffIntervalMillis(60000).build();
            //end::sniffer-interval
        }
        {
            //tag::sniff-on-failure
            SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
            RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
                    .setFailureListener(sniffOnFailureListener) // <1>
                    .build();
            Sniffer sniffer = Sniffer.builder(restClient)
                    .setSniffAfterFailureDelayMillis(30000) // <2>
                    .build();
            sniffOnFailureListener.setSniffer(sniffer); // <3>
            //end::sniff-on-failure
        }
        {
            //tag::sniffer-https
            RestClient restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"))
                    .build();
            HostsSniffer hostsSniffer = new ElasticsearchHostsSniffer(
                    restClient,
                    ElasticsearchHostsSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                    ElasticsearchHostsSniffer.Scheme.HTTPS);
            Sniffer sniffer = Sniffer.builder(restClient)
                    .setHostsSniffer(hostsSniffer).build();
            //end::sniffer-https
        }
        {
            //tag::sniff-request-timeout
            RestClient restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"))
                    .build();
            HostsSniffer hostsSniffer = new ElasticsearchHostsSniffer(
                    restClient,
                    TimeUnit.SECONDS.toMillis(5),
                    ElasticsearchHostsSniffer.Scheme.HTTP);
            Sniffer sniffer = Sniffer.builder(restClient)
                    .setHostsSniffer(hostsSniffer).build();
            //end::sniff-request-timeout
        }
        {
            //tag::custom-hosts-sniffer
            RestClient restClient = RestClient.builder(
                    new HttpHost("localhost", 9200, "http"))
                    .build();
            HostsSniffer hostsSniffer = new HostsSniffer() {
                @Override
                public List<HttpHost> sniffHosts() throws IOException {
                    return null; // <1>
                }
            };
            Sniffer sniffer = Sniffer.builder(restClient)
                    .setHostsSniffer(hostsSniffer).build();
            //end::custom-hosts-sniffer
        }
    }
}
