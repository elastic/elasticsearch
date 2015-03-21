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

import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
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
                .addCell("total", "text-align:right;desc:total field data usage")
                .endHeaders();
        return table;
    }

    private Table buildTable(final RestRequest request, final NodesStatsResponse nodeStatses) {
        Set<String> fieldNames = new HashSet<>();
        Map<NodeStats, ObjectLongMap<String>> nodesFields = new HashMap<>();

        // Collect all the field names so a new table can be built
        for (NodeStats ns : nodeStatses.getNodes()) {
            ObjectLongOpenHashMap<String> fields = ns.getIndices().getFieldData().getFields();
            nodesFields.put(ns, fields);
            if (fields != null) {
                for (String key : fields.keys().toArray(String.class)) {
                    fieldNames.add(key);
                }
            }
        }

        // The table must be rebuilt because it has dynamic headers based on the fields
        Table table = new Table();
        table.startHeaders()
                .addCell("id", "desc:node id")
                .addCell("host", "alias:h;desc:host name")
                .addCell("ip", "desc:ip address")
                .addCell("node", "alias:n;desc:node name")
                .addCell("total", "text-align:right;desc:total field data usage");
        // The table columns must be built dynamically since the number of fields is unknown
        for (String fieldName : fieldNames) {
            table.addCell(fieldName, "text-align:right;desc:" + fieldName + " field");
        }
        table.endHeaders();

        for (Map.Entry<NodeStats, ObjectLongMap<String>> statsEntry : nodesFields.entrySet()) {
            table.startRow();
            // add the node info and field data total before each individual field
            NodeStats ns = statsEntry.getKey();
            table.addCell(ns.getNode().id());
            table.addCell(ns.getNode().getHostName());
            table.addCell(ns.getNode().getHostAddress());
            table.addCell(ns.getNode().getName());
            table.addCell(ns.getIndices().getFieldData().getMemorySize());
            ObjectLongMap<String> fields = statsEntry.getValue();
            for (String fieldName : fieldNames) {
                table.addCell(new ByteSizeValue(fields == null ? 0L : fields.getOrDefault(fieldName, 0L)));
            }
            table.endRow();
        }

        return table;
    }
}
