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
package org.elasticsearch.shaded.test;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.nio.file.Path;

/**
 */
public class ShadedIT extends LuceneTestCase {

    public void testStartShadedNode() {
        ESLoggerFactory.getRootLogger().setLevel("ERROR");
        Path data = createTempDir();
        Settings settings = Settings.builder()
                .put("path.home", data.toAbsolutePath().toString())
                .put("node.mode", "local")
                .put("http.enabled", "false")
                .build();
        NodeBuilder builder = NodeBuilder.nodeBuilder().data(true).settings(settings).loadConfigSettings(false).local(true);
        try (Node node = builder.node()) {
            Client client = node.client();
            client.admin().indices().prepareCreate("test").get();
            client.prepareIndex("test", "foo").setSource("{ \"field\" : \"value\" }").get();
            client.admin().indices().prepareRefresh().get();
            SearchResponse response = client.prepareSearch("test").get();
            assertEquals(response.getHits().getTotalHits(), 1l);
        }

    }
}
