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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

public class IndexDeleteByQueryResponseTests extends ESTestCase {

    @Test
    public void testIncrements() {
        String indexName = randomAsciiOfLength(5);

        // Use randomInt to prevent range overflow
        long found = Math.abs(randomInt());
        long deleted = Math.abs(randomInt());
        long missing = Math.abs(randomInt());
        long failed = Math.abs(randomInt());

        IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse(indexName, found, deleted, missing, failed);
        assertThat(response.getIndex(), equalTo(indexName));
        assertThat(response.getFound(), equalTo(found));
        assertThat(response.getDeleted(), equalTo(deleted));
        assertThat(response.getMissing(), equalTo(missing));
        assertThat(response.getFailed(), equalTo(failed));

        response.incrementFound();
        response.incrementDeleted();
        response.incrementMissing();
        response.incrementFailed();

        assertThat(response.getFound(), equalTo(found + 1));
        assertThat(response.getDeleted(), equalTo(deleted + 1));
        assertThat(response.getMissing(), equalTo(missing + 1));
        assertThat(response.getFailed(), equalTo(failed + 1));

        // Use randomInt to prevent range overflow
        long inc = randomIntBetween(0, 1000);
        response.incrementFound(inc);
        response.incrementDeleted(inc);
        response.incrementMissing(inc);
        response.incrementFailed(inc);

        assertThat(response.getFound(), equalTo(found + 1 + inc));
        assertThat(response.getDeleted(), equalTo(deleted + 1 + inc));
        assertThat(response.getMissing(), equalTo(missing + 1 + inc));
        assertThat(response.getFailed(), equalTo(failed + 1 + inc));
    }

    @Test
    public void testNegativeCounters() {
        assumeTrue("assertions must be enable for this test to pass", assertionsEnabled());
        try {
            new IndexDeleteByQueryResponse("index", -1L, 0L, 0L, 0L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'found' cannot be negative"), equalTo(true));
        }

        try {
            new IndexDeleteByQueryResponse("index", 0L, -1L, 0L, 0L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'deleted' cannot be negative"), equalTo(true));
        }

        try {
            new IndexDeleteByQueryResponse("index", 0L, 0L, -1L, 0L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'missing' cannot be negative"), equalTo(true));
        }

        try {
            new IndexDeleteByQueryResponse("index", 0L, 0L, 0L, -1L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'failed' cannot be negative"), equalTo(true));
        }
    }

    @Test
    public void testNegativeIncrements() {
        assumeTrue("assertions must be enable for this test to pass", assertionsEnabled());
        try {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.incrementFound(-10L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'found' cannot be negative"), equalTo(true));
        }

        try {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.incrementDeleted(-10L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'deleted' cannot be negative"), equalTo(true));
        }

        try {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.incrementMissing(-10L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'missing' cannot be negative"), equalTo(true));
        }

        try {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.incrementFailed(-1L);
            fail("should have thrown an assertion error concerning the negative counter");
        } catch (AssertionError e) {
            assertThat("message contains error about a negative counter: " + e.getMessage(),
                    e.getMessage().contains("counter 'failed' cannot be negative"), equalTo(true));
        }
    }

    @Test
    public void testSerialization() throws Exception {
        IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse(randomAsciiOfLength(5), Math.abs(randomLong()), Math.abs(randomLong()), Math.abs(randomLong()), Math.abs(randomLong()));
        Version testVersion = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(testVersion);
        response.writeTo(output);

        StreamInput streamInput = StreamInput.wrap(output.bytes());
        streamInput.setVersion(testVersion);
        IndexDeleteByQueryResponse deserializedResponse = new IndexDeleteByQueryResponse();
        deserializedResponse.readFrom(streamInput);

        assertThat(deserializedResponse.getIndex(), equalTo(response.getIndex()));
        assertThat(deserializedResponse.getFound(), equalTo(response.getFound()));
        assertThat(deserializedResponse.getDeleted(), equalTo(response.getDeleted()));
        assertThat(deserializedResponse.getMissing(), equalTo(response.getMissing()));
        assertThat(deserializedResponse.getFailed(), equalTo(response.getFailed()));
    }

}
