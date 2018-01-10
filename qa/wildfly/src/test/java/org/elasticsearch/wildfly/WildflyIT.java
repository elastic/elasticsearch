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

package org.elasticsearch.wildfly;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WildflyIT extends LuceneTestCase {

    public void testTransportClient() throws URISyntaxException, IOException {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            final String str = String.format(
                    Locale.ROOT,
                    "http://localhost:%d/wildfly-%s%s/transport/employees/1",
                    Integer.parseInt(System.getProperty("tests.jboss.http.port")),
                    Version.CURRENT,
                    Build.CURRENT.isSnapshot() ? "-SNAPSHOT" : "");
            final HttpPut put = new HttpPut(new URI(str));
            final String body;
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.field("first_name", "John");
                    builder.field("last_name", "Smith");
                    builder.field("age", 25);
                    builder.field("about", "I love to go rock climbing");
                    builder.startArray("interests");
                    {
                        builder.value("sports");
                        builder.value("music");
                    }
                    builder.endArray();
                }
                builder.endObject();
                body = builder.string();
            }
            put.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = client.execute(put)) {
                assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
            }

            final HttpGet get = new HttpGet(new URI(str));
            try (
                    CloseableHttpResponse response = client.execute(get);
                    XContentParser parser =
                            JsonXContent.jsonXContent.createParser(
                                    new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
                                    response.getEntity().getContent())) {
                final Map<String, Object> map = parser.map();
                assertThat(map.get("first_name"), equalTo("John"));
                assertThat(map.get("last_name"), equalTo("Smith"));
                assertThat(map.get("age"), equalTo(25));
                assertThat(map.get("about"), equalTo("I love to go rock climbing"));
                final Object interests = map.get("interests");
                assertThat(interests, instanceOf(List.class));
                @SuppressWarnings("unchecked") final List<String> interestsAsList = (List<String>) interests;
                assertThat(interestsAsList, containsInAnyOrder("sports", "music"));
            }
        }
    }

}
