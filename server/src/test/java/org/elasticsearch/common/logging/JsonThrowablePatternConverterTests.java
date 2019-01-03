package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
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

public class JsonThrowablePatternConverterTests extends ESTestCase {

    //TODO To be extended and cleaned up
    public void testStacktraceWithJson() throws IOException {
        LogManager.getLogger().info("asdf");

        String json = "{\n" +
            "  \"terms\" : {\n" +
            "    \"user\" : [\n" +
            "      \"u1\",\n" +
            "      \"u2\",\n" +
            "      \"u3\"\n" +
            "    ],\n" +
            "    \"boost\" : 1.0\n" +
            "  }\n" +
            "}";
        LogEvent event = Log4jLogEvent.newBuilder()
            .setMessage(new SimpleMessage("message"))
            .setThrown(new Exception(json))
            .build();


        JsonThrowablePatternConverter converter = JsonThrowablePatternConverter.newInstance(null, null);

        StringBuilder builder = new StringBuilder();
        converter.format(event, builder);

        String x = "{\"type\": \"console\", \"timestamp\": \"2019-01-03T16:30:53,058+0100\", \"level\": \"DEBUG\", \"class\": \"o.e.a.s" +
            ".TransportSearchAction\", \"cluster.name\": \"clustername\", \"node.name\": \"node-0\", \"cluster.uuid\": " +
            "\"OG5MkvOrR9azuClJhWvy6Q\", \"node.id\": \"VTShUqmcQG6SzeKY5nn7qA\",  \"message\": \"msg msg\" " + builder.toString() + "}";
        JsonLogs jsonLogs = new JsonLogs(new BufferedReader(new StringReader(x)));

//        for (JsonLogLine jsonLogLine : jsonLogs) {
//            assertThat
//        }
    }

}
