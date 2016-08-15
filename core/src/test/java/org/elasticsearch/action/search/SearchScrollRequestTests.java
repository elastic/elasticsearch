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

package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SearchScrollRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        SearchScrollRequest searchScrollRequest = createSearchScrollRequest();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            searchScrollRequest.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                SearchScrollRequest deserializedRequest = new SearchScrollRequest();
                deserializedRequest.readFrom(in);
                assertEquals(deserializedRequest, searchScrollRequest);
                assertEquals(deserializedRequest.hashCode(), searchScrollRequest.hashCode());
                assertNotSame(deserializedRequest, searchScrollRequest);
            }
        }
    }

    public void testInternalScrollSearchRequestSerialization() throws IOException {
        SearchScrollRequest searchScrollRequest = createSearchScrollRequest();
        InternalScrollSearchRequest internalScrollSearchRequest = new InternalScrollSearchRequest(searchScrollRequest, randomLong());
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            internalScrollSearchRequest.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                InternalScrollSearchRequest deserializedRequest = new InternalScrollSearchRequest();
                deserializedRequest.readFrom(in);
                assertEquals(deserializedRequest.id(), internalScrollSearchRequest.id());
                assertEquals(deserializedRequest.scroll(), internalScrollSearchRequest.scroll());
                assertNotSame(deserializedRequest, internalScrollSearchRequest);
            }
        }
    }

    public void testEqualsAndHashcode() {
        SearchScrollRequest firstSearchScrollRequest = createSearchScrollRequest();
        assertNotNull("search scroll request is equal to null", firstSearchScrollRequest);
        assertNotEquals("search scroll request  is equal to incompatible type", firstSearchScrollRequest, "");
        assertEquals("search scroll request is not equal to self", firstSearchScrollRequest, firstSearchScrollRequest);
        assertEquals("same source builder's hashcode returns different values if called multiple times",
                firstSearchScrollRequest.hashCode(), firstSearchScrollRequest.hashCode());

        SearchScrollRequest secondSearchScrollRequest = copyRequest(firstSearchScrollRequest);
        assertEquals("search scroll request  is not equal to self", secondSearchScrollRequest, secondSearchScrollRequest);
        assertEquals("search scroll request is not equal to its copy", firstSearchScrollRequest, secondSearchScrollRequest);
        assertEquals("search scroll request is not symmetric", secondSearchScrollRequest, firstSearchScrollRequest);
        assertEquals("search scroll request copy's hashcode is different from original hashcode",
                firstSearchScrollRequest.hashCode(), secondSearchScrollRequest.hashCode());

        SearchScrollRequest thirdSearchScrollRequest = copyRequest(secondSearchScrollRequest);
        assertEquals("search scroll request is not equal to self", thirdSearchScrollRequest, thirdSearchScrollRequest);
        assertEquals("search scroll request is not equal to its copy", secondSearchScrollRequest, thirdSearchScrollRequest);
        assertEquals("search scroll request copy's hashcode is different from original hashcode",
                secondSearchScrollRequest.hashCode(), thirdSearchScrollRequest.hashCode());
        assertEquals("equals is not transitive", firstSearchScrollRequest, thirdSearchScrollRequest);
        assertEquals("search scroll request copy's hashcode is different from original hashcode",
                firstSearchScrollRequest.hashCode(), thirdSearchScrollRequest.hashCode());
        assertEquals("equals is not symmetric", thirdSearchScrollRequest, secondSearchScrollRequest);
        assertEquals("equals is not symmetric", thirdSearchScrollRequest, firstSearchScrollRequest);

        boolean changed = false;
        if (randomBoolean()) {
            secondSearchScrollRequest.scrollId(randomAsciiOfLengthBetween(3, 10));
            if (secondSearchScrollRequest.scrollId().equals(firstSearchScrollRequest.scrollId()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchScrollRequest.scroll(randomPositiveTimeValue());
            if (secondSearchScrollRequest.scroll().equals(firstSearchScrollRequest.scroll()) == false) {
                changed = true;
            }
        }
        
        if (changed) {
            assertNotEquals(firstSearchScrollRequest, secondSearchScrollRequest);
            assertNotEquals(firstSearchScrollRequest.hashCode(), secondSearchScrollRequest.hashCode());
        } else {
            assertEquals(firstSearchScrollRequest, secondSearchScrollRequest);
            assertEquals(firstSearchScrollRequest.hashCode(), secondSearchScrollRequest.hashCode());
        }
    }
    
    public static SearchScrollRequest createSearchScrollRequest() {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(randomAsciiOfLengthBetween(3, 10));
        searchScrollRequest.scroll(randomPositiveTimeValue());
        return searchScrollRequest;
    }

    private static SearchScrollRequest copyRequest(SearchScrollRequest searchScrollRequest) {
        SearchScrollRequest result = new SearchScrollRequest();
        result.scrollId(searchScrollRequest.scrollId());
        result.scroll(searchScrollRequest.scroll());
        return result;
    }
}
