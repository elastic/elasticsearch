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
package org.elasticsearch.plugin.example;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Example of adding a cat action with a plugin.
 */
public class ExampleCatAction extends AbstractCatAction {
    private final ExamplePluginConfiguration config;

    public ExampleCatAction(Settings settings, RestController controller, ExamplePluginConfiguration config) {
        super(settings);
        this.config = config;
        controller.registerHandler(GET, "/_cat/configured_example", this);
    }

    @Override
    public String getName() {
        return "example_cat_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        Table table = getTableWithHeader(request);
        table.startRow();
        table.addCell(config.getTestConfig());
        table.endRow();
        return channel -> {
            try {
                channel.sendResponse(RestTable.buildResponse(table, channel));
            } catch (final Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append(documentation());
    }

    public static String documentation() {
        return "/_cat/configured_example\n";
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("test", "desc:test");
        table.endHeaders();
        return table;
    }
}
