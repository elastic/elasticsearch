/*
 * Licensed to ElasticSearch and Shay Banon under one
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
package org.elasticsearch.rest.action.cat;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 *
 */
public class RestAliasAction extends AbstractCatAction {

    @Inject
    public RestAliasAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/aliases", this);
        controller.registerHandler(GET, "/_cat/aliases/{alias}", this);
    }


    @Override
    void doRequest(final RestRequest request, final RestChannel channel) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.filterMetaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        clusterStateRequest.filterAll().filterMetaData(false);

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {

            @Override
            public void onResponse(ClusterStateResponse response) {
                try {
                    Table tab = buildTable(request, response);
                    channel.sendResponse(RestTable.buildResponse(tab, request, channel));
                } catch (Throwable e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat_alias");
        sb.append("/_cat_alias/{alias}");
    }

    @Override
    Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("alias", "desc:alias name");
        table.addCell("index", "desc:index alias points to");
        table.addCell("filter", "desc:filter");
        table.addCell("index_routing", "desc:index routing");
        table.addCell("search_routing", "desc:search routing");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, ClusterStateResponse response) {
        Table table = getTableWithHeader(request);

        for (ObjectObjectCursor<String, ImmutableOpenMap<String, AliasMetaData>> cursor : response.getState().getMetaData().aliases()) {
            String aliasName = cursor.key;
            Iterator<ObjectObjectCursor<String,AliasMetaData>> iterator = cursor.value.iterator();
            while (iterator.hasNext()) {
                ObjectObjectCursor<String, AliasMetaData> iteratorCursor = iterator.next();
                String indexName = iteratorCursor.key;
                AliasMetaData aliasMetaData = iteratorCursor.value;

                table.startRow();
                table.addCell(aliasName);
                table.addCell(indexName);
                table.addCell(aliasMetaData.filteringRequired() ? "*" : "-");
                String indexRouting = Strings.hasLength(aliasMetaData.indexRouting()) ? aliasMetaData.indexRouting() : "-";
                table.addCell(indexRouting);
                String searchRouting = Strings.hasLength(aliasMetaData.searchRouting()) ? aliasMetaData.searchRouting() : "-";
                table.addCell(searchRouting);
                table.endRow();
            }
        }

        return table;
    }

}
