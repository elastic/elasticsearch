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

package org.elasticsearch.rest.action.cat;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about the size of fielddata fields per node
 */
public class RestFielddataAction extends AbstractCatAction {

    @Inject
    public RestFielddataAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cat/fielddata", this);
        controller.registerHandler(GET, "/_cat/fielddata/{fields}", this);
    }

    @Override
    void doRequest(final RestRequest request, final RestChannel channel, final Client client) {

        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.clear();
        nodesStatsRequest.indices(true);
        String[] fields = request.paramAsStringArray("fields", null);
        nodesStatsRequest.indices().fieldDataFields(fields == null ? new String[] {"*"} : fields);

        client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesStatsResponse nodeStatses) throws Exception {
                return RestTable.buildResponse(buildTable(request, nodeStatses), channel);
            }
        });
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/fielddata\n");
        sb.append("/_cat/fielddata/{fields}\n");
    }

    @Override
    Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders()
                .addCell("id", "desc:node id")
                .addCell("host", "alias:h;desc:host name")
                .addCell("ip", "desc:ip address")
                .addCell("node", "alias:n;desc:node name")
                .addCell("total", "text-align:right;desc:total field data usage on the node");
        
        if (isFieldLevel(request)) {
            table.addCell("field", "text-align:right;desc:field name");
            table.addCell("size", "text-align:right;desc:field data usage");
        }
        
        table.endHeaders();
        return table;
    }

    private Table buildTable(final RestRequest request, final NodesStatsResponse nodeStatses) {
        Table table = getTableWithHeader(request);

        for (NodeStats ns : nodeStatses.getNodes()) {   
            if (isFieldLevel(request)) 
                addFieldLevelRows(table, ns);                
            else
                addNodeLevelRow(table, ns);                     
        }        
        return table;
    }

    private void addNodeLevelRow(Table table, NodeStats ns) {
        table.startRow();
        addNodeInfoAndTotals(table, ns);                
        table.endRow();
    }

    private void addFieldLevelRows(Table table, NodeStats ns) {
        ObjectLongHashMap<String> fields = ns.getIndices().getFieldData().getFields();
        if (fields != null) {                
            for (ObjectCursor<String> key : fields.keys()) {
                String fieldName = key.value;
                
                table.startRow();
                addNodeInfoAndTotals(table, ns);                        
                table.addCell(fieldName);
                table.addCell(new ByteSizeValue(fields.getOrDefault(fieldName, 0L)));
                table.endRow();                     
            }
        }
    }

    private void addNodeInfoAndTotals(Table table, NodeStats ns) {
        table.addCell(ns.getNode().id());
        table.addCell(ns.getNode().getHostName());
        table.addCell(ns.getNode().getHostAddress());
        table.addCell(ns.getNode().getName());
        table.addCell(ns.getIndices().getFieldData().getMemorySize());
    }
        
    private boolean isFieldLevel(final RestRequest request) {
        return request.hasParam("fields") ;
    }
}
