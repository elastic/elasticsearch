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

package org.elasticsearch.util.json;

import org.elasticsearch.util.io.FastCharArrayWriter;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonBuilderTests {

    @Test public void verifyReuseJsonGenerator() throws Exception {
        FastCharArrayWriter writer = new FastCharArrayWriter();
        org.codehaus.jackson.JsonGenerator generator = Jackson.defaultJsonFactory().createJsonGenerator(writer);
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();

        assertThat(writer.toStringTrim(), equalTo("{\"test\":\"value\"}"));

        // try again...
        writer.reset();
        generator.writeStartObject();
        generator.writeStringField("test", "value");
        generator.writeEndObject();
        generator.flush();
        // we get a space at the start here since it thinks we are not in the root object (fine, we will ignore it in the real code we use)
        assertThat(writer.toStringTrim(), equalTo("{\"test\":\"value\"}"));
    }

    @Test public void testSimpleJacksonGenerator() throws Exception {
        JsonBuilder builder = new JsonBuilder();
        assertThat(builder.startObject().field("test", "value").endObject().string(), equalTo("{\"test\":\"value\"}"));
        builder.reset();
        assertThat(builder.startObject().field("test", "value").endObject().string(), equalTo("{\"test\":\"value\"}"));
    }
}
