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

package org.elasticsearch.stresstest.gcbehavior;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class FilterCacheGcStress {

    public static void main(String[] args) {

        Settings settings = ImmutableSettings.EMPTY;

        Node node = NodeBuilder.nodeBuilder().settings(settings).node();
        final Client client = node.client();

        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        final AtomicBoolean stop = new AtomicBoolean();

        Thread indexingThread = new Thread() {
            @Override
            public void run() {
                while (!stop.get()) {
                    client.prepareIndex("test", "type1").setSource("field", System.currentTimeMillis()).execute().actionGet();
                }
            }
        };
        indexingThread.start();

        Thread searchThread = new Thread() {
            @Override
            public void run() {
                while (!stop.get()) {
                    client.prepareSearch()
                            .setQuery(filteredQuery(matchAllQuery(), rangeFilter("field").from(System.currentTimeMillis() - 1000000)))
                            .execute().actionGet();
                }
            }
        };

        searchThread.start();
    }
}