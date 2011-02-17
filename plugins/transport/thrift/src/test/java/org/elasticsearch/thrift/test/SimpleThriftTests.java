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

package org.elasticsearch.thrift.test;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.thrift.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleThriftTests {

    private Node node;

    private TTransport transport;

    private Rest.Client client;

    @BeforeMethod public void setup() throws IOException, TTransportException {
        node = nodeBuilder().settings(settingsBuilder().put("gateway.type", "none")).node();
        transport = new TSocket("localhost", 9500);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Rest.Client(protocol);
        transport.open();
    }

    @AfterMethod public void tearDown() {
        transport.close();
        node.close();
    }

    @Test public void testSimpleApis() throws Exception {
        RestRequest request = new RestRequest(Method.POST, "/test/type1");
        request.setBody(ByteBuffer.wrap(XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().copiedBytes()));
        RestResponse response = client.execute(request);
        Map<String, Object> map = parseBody(response);
        assertThat(response.getStatus(), equalTo(Status.CREATED));
        assertThat(map.get("ok").toString(), equalTo("true"));
        assertThat(map.get("_index").toString(), equalTo("test"));
        assertThat(map.get("_type").toString(), equalTo("type1"));

        request = new RestRequest(Method.GET, "/_cluster/health");
        response = client.execute(request);
        assertThat(response.getStatus(), equalTo(Status.OK));
    }

    private Map<String, Object> parseBody(RestResponse response) throws IOException {
        return XContentFactory.xContent(XContentType.JSON).createParser(response.BufferForBody().array(), response.BufferForBody().arrayOffset(), response.BufferForBody().remaining()).map();
    }
}
