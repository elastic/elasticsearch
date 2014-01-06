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

package org.elasticsearch.document;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 *
 */
public class AliasedIndexDocumentActionsTests extends DocumentActionsTests {

    protected void createIndex() {
        logger.info("Creating index [test1] with alias [test]");
        try {
            client().admin().indices().prepareDelete("test1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        logger.info("--> creating index test");
        client().admin().indices().create(createIndexRequest("test1").settings(settingsBuilder().putArray("index.aliases", "test"))).actionGet();
    }

    @Override
    protected String getConcreteIndexName() {
        return "test1";
    }
}
