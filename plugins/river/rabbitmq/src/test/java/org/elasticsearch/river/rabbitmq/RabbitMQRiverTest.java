/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.river.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * @author kimchy (shay.banon)
 */
public class RabbitMQRiverTest {

    public static void main(String[] args) throws Exception {

        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();

        node.client().prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder().startObject().field("type", "rabbitmq").endObject()).execute().actionGet();

        ConnectionFactory cfconn = new ConnectionFactory();
        cfconn.setHost("localhost");
        cfconn.setPort(AMQP.PROTOCOL.PORT);
        Connection conn = cfconn.newConnection();

        Channel ch = conn.createChannel();
        ch.exchangeDeclare("elasticsearch", "direct", true);
        ch.queueDeclare("elasticsearch", true, false, false, null);

        String message = "{ \"index\" : { \"index\" : \"test\", \"type\" : \"type1\", \"id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
                "{ \"delete\" : { \"index\" : \"test\", \"type\" : \"type1\", \"id\" : \"2\" } }\n" +
                "{ \"create\" : { \"index\" : \"test\", \"type\" : \"type1\", \"id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }";

        ch.basicPublish("elasticsearch", "elasticsearch", null, message.getBytes());

        ch.close();
        conn.close();

        Thread.sleep(100000);
    }
}
