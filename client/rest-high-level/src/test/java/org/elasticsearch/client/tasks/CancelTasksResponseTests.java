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

package org.elasticsearch.client.tasks;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CancelTasksResponseTests extends ESTestCase {

    String response = "{\n" +
        " \"node_failures\" : [\n" +
        "    {\n" +
        "      \"type\" : \"failed_node_exception\",\n" +
        "      \"reason\" : \"Failed node [AAA]\",\n" +
        "      \"node_id\" : \"AAA\",\n" +
        "      \"caused_by\" : {\n" +
        "        \"type\" : \"no_such_node_exception\",\n" +
        "        \"reason\" : \"No such node [AAA]\",\n" +
        "        \"node_id\" : \"AAA\"\n" +
        "      }\n" +
        "    }\n" +
        "  ]," +
        "  \"task_failures\" : [\n"+
        "    {\n"+
        "      \"task_id\" : 1186,\n"+
        "      \"node_id\" : \"area51node\",\n"+
        "      \"status\" : \"INTERNAL_SERVER_ERROR\",\n"+
        "      \"reason\" : {\n"+
        "        \"type\" : \"illegal_state_exception\",\n"+
        "        \"reason\" : \"task with id 1186 is already cancelled\"\n"+
        "      }\n"+
        "    }\n"+
        "  ],\n"+
        "  \"nodes\" : {\n" +
        "    \"BBB\" : {\n" +
        "      \"name\" : \"instance-0000000004\",\n" +
        "      \"transport_address\" : \"192.168.1.1:19987\",\n" +
        "      \"host\" : \"192.168.1.1\",\n" +
        "      \"ip\" : \"192.168.1.1\",\n" +
        "      \"roles\" : [\n" +
        "        \"master\",\n" +
        "        \"data\",\n" +
        "        \"ingest\"\n" +
        "      ],\n" +
        "      \"attributes\" : {\n" +
        "        \"logical_availability_zone\" : \"zone-0\",\n" +
        "        \"server_name\" : \"upupaPowerdome\",\n" +
        "        \"availability_zone\" : \"us-east-1e\",\n" +
        "        \"region\" : \"ita-molise-1\" \n" +
        "      },\n" +
        "      \"tasks\" : {\n" +
        "        \"BBB:1481971\" : {\n" +
        "          \"node\" : \"BBB\",\n" +
        "          \"id\" : 1481971,\n" +
        "          \"type\" : \"transport\",\n" +
        "          \"action\" : \"indices:data/write/reindex\",\n" +
        "          \"status\" : { \n" +
        "            \"time\": \"now\",\n"+
        "            \"node_count\" : [1,2,3],\n"+
        "            \"node_stats\" : [[1,2],[3,4]]\n"+
        "          },\n" +
        "          \"start_time_in_millis\" : 1565108484002,\n" +
        "          \"running_time_in_nanos\" : 18328639067,\n" +
        "          \"cancellable\" : true,\n" +
        "          \"headers\" : { }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";


    public void testFromXContent() throws IOException {

        XContentParser parser = JsonXContent.jsonXContent.createParser(
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE,
            response);

        CancelTasksResponse response = CancelTasksResponse.fromXContent(parser);

        NodeData nodeData = new NodeData("BBB");
        nodeData.setName("instance-0000000004");
        nodeData.setTransportAddress("192.168.1.1:19987");
        nodeData.setHost("192.168.1.1");
        nodeData.setIp("192.168.1.1");
        nodeData.setRoles(List.of("master", "data", "ingest"));
        nodeData.setAttributes(
            Map.of(
                "logical_availability_zone", "zone-0",
                "server_name", "upupaPowerdome",
                "availability_zone", "us-east-1e",
                "region", "ita-molise-1"
            )
        );

        TaskInfo taskInfo = new TaskInfo(new TaskId("BBB:1481971"));
        taskInfo.setType("transport");
        taskInfo.setAction("indices:data/write/reindex");
        taskInfo.setStartTime(1565108484002L);
        taskInfo.setRunningTimeNanos(18328639067L);
        taskInfo.setStatus(
            Map.of(
                "time", "now",
                "node_count", new ArrayList<>(List.of(1,2,3)),
                "node_stats", new ArrayList<>(List.of(new ArrayList<>(List.of(1,2)),new ArrayList<>(List.of(3,4))))
            )
        );
        taskInfo.setCancellable(true);
        taskInfo.setHeaders(Map.of());

        nodeData.setTasks(List.of(taskInfo));

        ElasticsearchException causer = new ElasticsearchException(
            "Elasticsearch exception [type=no_such_node_exception, reason=No such node [AAA]]"
        );
        ElasticsearchException caused = new ElasticsearchException(
            "Elasticsearch exception [type=failed_node_exception, reason=Failed node [AAA]]", causer
        );

        ElasticsearchException reason = new ElasticsearchException(
            "Elasticsearch exception [type=illegal_state_exception, reason=task with id 1186 is already cancelled]"
        );

        TaskOperationFailure tof = new TaskOperationFailure(
            "area51node",
            1186L,
            "INTERNAL_SERVER_ERROR",
            reason
        );
        CancelTasksResponse expected = new CancelTasksResponse(
            List.of(nodeData),
            List.of(tof),
            List.of(caused)
        );

        assertEquals(expected, response);

    }
}
