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
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
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

@TestRuleLimitSysouts.Limit(bytes = 14000)
public class WildflyIT extends LuceneTestCase {

    private Logger logger = LogManager.getLogger(WildflyIT.class);

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/45625")
    public void testRestClient() throws URISyntaxException, IOException {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            final String str = String.format(
                Locale.ROOT,
                "%s/employees/1",
                System.getProperty("tests.jboss.root")
            );
            logger.info("Connecting to uri: " + str);
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
                body = Strings.toString(builder);
            }
            put.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = client.execute(put)) {
                int status = response.getStatusLine().getStatusCode();
                assertThat("expected a 201 response but got: " + status + " - body: " + EntityUtils.toString(response.getEntity()),
                        status, equalTo(201));
            }

            final HttpGet get = new HttpGet(new URI(str));
            try (
                    CloseableHttpResponse response = client.execute(get);
                    XContentParser parser =
                            JsonXContent.jsonXContent.createParser(
                                    new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
                                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
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
