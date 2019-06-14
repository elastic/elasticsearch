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

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkRequestParserTests extends ESTestCase {

    public void testIndexRequest() throws IOException {
        BytesArray request = new BytesArray("{ \"index\":{ \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                indexRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", indexRequest.index());
                    assertEquals("bar", indexRequest.id());
                    parsed.set(true);
                },
                req -> fail(), req -> fail());
        assertTrue(parsed.get());
    }

    public void testDeleteRequest() throws IOException {
        BytesArray request = new BytesArray("{ \"delete\":{ \"_id\": \"bar\" } }\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                req -> fail(), req -> fail(),
                deleteRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", deleteRequest.index());
                    assertEquals("bar", deleteRequest.id());
                    parsed.set(true);
                });
        assertTrue(parsed.get());
    }

    public void testUpdateRequest() throws IOException {
        BytesArray request = new BytesArray("{ \"update\":{ \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                req -> fail(),
                updateRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", updateRequest.index());
                    assertEquals("bar", updateRequest.id());
                    parsed.set(true);
                },
                req -> fail());
        assertTrue(parsed.get());
    }

    public void testBarfOnLackOfTrailingNewline() throws IOException {
        BytesArray request = new BytesArray("{ \"index\":{ \"_id\": \"bar\" } }\n{}");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                        indexRequest -> fail(), req -> fail(), req -> fail()));
        assertEquals("The bulk request must be terminated by a newline [\\n]", e.getMessage());
    }

    public void testFailOnExplicitIndex() throws IOException {
        BytesArray request = new BytesArray("{ \"index\":{ \"_index\": \"foo\", \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(randomBoolean());
        
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> parser.parse(request, null, null, null, null, null, false, XContentType.JSON,
                        req -> fail(), req -> fail(), req -> fail()));
        assertEquals("explicit index in bulk is not allowed", ex.getMessage());
    }

    public void testTypeWarning() throws IOException {
        BytesArray request = new BytesArray("{ \"index\":{ \"_type\": \"quux\", \"_id\": \"bar\" } }\n{}\n");
        BulkRequestParser parser = new BulkRequestParser(true);
        final AtomicBoolean parsed = new AtomicBoolean();
        parser.parse(request, "foo", null, null, null, null, false, XContentType.JSON,
                indexRequest -> {
                    assertFalse(parsed.get());
                    assertEquals("foo", indexRequest.index());
                    assertEquals("bar", indexRequest.id());
                    parsed.set(true);
                },
                req -> fail(), req -> fail());
        assertTrue(parsed.get());

        assertWarnings(RestBulkAction.TYPES_DEPRECATION_MESSAGE);
    }

}
