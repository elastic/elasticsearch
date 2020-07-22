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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class JsonThrowablePatternConverterTests extends ESTestCase {
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private JsonThrowablePatternConverter converter = JsonThrowablePatternConverter.newInstance(null, null);

    public void testNoStacktrace() throws IOException {
        LogEvent event = Log4jLogEvent.newBuilder()
                                      .build();
        String result = format(event);

        JsonLogLine jsonLogLine = JsonLogsStream.from(new BufferedReader(new StringReader(result)))
                                                .findFirst()
                                                .orElseThrow(() -> new AssertionError("no logs parsed"));

        assertThat(jsonLogLine.stacktrace(), Matchers.nullValue());
    }

    public void testStacktraceWithJson() throws IOException {

        String json = "{" + LINE_SEPARATOR +
            "  \"terms\" : {" + LINE_SEPARATOR +
            "    \"user\" : [" + LINE_SEPARATOR +
            "      \"u1\"," + LINE_SEPARATOR +
            "      \"u2\"," + LINE_SEPARATOR +
            "      \"u3\"" + LINE_SEPARATOR +
            "    ]," + LINE_SEPARATOR +
            "    \"boost\" : 1.0" + LINE_SEPARATOR +
            "  }" + LINE_SEPARATOR +
            "}";
        Exception thrown = new Exception(json);
        LogEvent event = Log4jLogEvent.newBuilder()
                                      .setMessage(new SimpleMessage("message"))
                                      .setThrown(thrown)
                                      .build();


        String result = format(event);

        //confirms exception is correctly parsed

        JsonLogLine jsonLogLine = JsonLogsStream.from(new BufferedReader(new StringReader(result)), JsonLogLine.ES_LOG_LINE)
                                                .findFirst()
                                                .orElseThrow(() -> new AssertionError("no logs parsed"));

        int jsonLength = json.split(LINE_SEPARATOR).length;
        int stacktraceLength = thrown.getStackTrace().length;
        assertThat("stacktrace should formatted in multiple lines. JsonLogLine= " + jsonLogLine+" result= "+result,
            jsonLogLine.stacktrace().size(), equalTo(jsonLength + stacktraceLength));
    }

    private String format(LogEvent event) {
        StringBuilder builder = new StringBuilder();
        converter.format(event, builder);
        String jsonStacktraceElement = builder.toString();

        return "{\"type\": \"console\", \"timestamp\": \"2019-01-03T16:30:53,058+0100\", \"level\": \"DEBUG\", " +
            "\"component\": \"o.e.a.s.TransportSearchAction\", \"cluster.name\": \"clustername\", \"node.name\": \"node-0\", " +
            "\"cluster.uuid\": \"OG5MkvOrR9azuClJhWvy6Q\", \"node.id\": \"VTShUqmcQG6SzeKY5nn7qA\",  \"message\": \"msg msg\" " +
            jsonStacktraceElement + "}";
    }
}
