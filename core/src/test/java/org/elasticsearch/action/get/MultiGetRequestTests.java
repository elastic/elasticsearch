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

package org.elasticsearch.action.get;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;

public class MultiGetRequestTests extends ESTestCase {

    public void testAddWithInvalidSourceValueIsRejected() throws Exception {
        String sourceValue = randomFrom("on", "off", "0", "1");
        XContentParser parser = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .startArray("docs")
                    .startObject()
                        .field("_source", sourceValue)
                    .endObject()
                .endArray()
            .endObject()
        );

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> multiGetRequest.add
            (randomAsciiOfLength(5), randomAsciiOfLength(3), null, FetchSourceContext.FETCH_SOURCE, null, parser, true));
        assertEquals("Failed to parse value [" + sourceValue + "] as only [true] or [false] are allowed.", ex.getMessage());
    }

    public void testAddWithValidSourceValueIsAccepted() throws Exception {
        XContentParser parser = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .startArray("docs")
                    .startObject()
                        .field("_source", randomFrom("false", "true"))
                    .endObject()
                    .startObject()
                        .field("_source", randomBoolean())
                    .endObject()
                .endArray()
            .endObject()
        );

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add(
            randomAsciiOfLength(5), randomAsciiOfLength(3), null, FetchSourceContext.FETCH_SOURCE, null, parser, true);

        assertEquals(2, multiGetRequest.getItems().size());
    }
}
