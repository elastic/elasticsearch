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

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.builder.SearchSourceBuilderTests.createSearchSourceBuilder;

public class SearchRequestTests extends ESTestCase {

    private static NamedWriteableRegistry namedWriteableRegistry;

    @BeforeClass
    public static void beforeClass() {
        IndicesModule indicesModule = new IndicesModule(emptyList()) {
            @Override
            protected void configure() {
                bindMapperExtension();
            }
        };
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList()) {
            @Override
            protected void configureSearch() {
                // Skip me
            }
        };
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
    }

    public void testSerialization() throws Exception {
        SearchRequest searchRequest = createSearchRequest();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            searchRequest.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                SearchRequest deserializedRequest = new SearchRequest();
                deserializedRequest.readFrom(in);
                assertEquals(deserializedRequest, searchRequest);
                assertEquals(deserializedRequest.hashCode(), searchRequest.hashCode());
                assertNotSame(deserializedRequest, searchRequest);
            }
        }
    }

    public void testIllegalArguments() {
        SearchRequest searchRequest = new SearchRequest();
        assertNotNull(searchRequest.indices());
        assertNotNull(searchRequest.indicesOptions());
        assertNotNull(searchRequest.types());
        assertNotNull(searchRequest.searchType());

        NullPointerException e = expectThrows(NullPointerException.class, () -> searchRequest.indices((String[]) null));
        assertEquals("indices must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> searchRequest.indices((String) null));
        assertEquals("index must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.indicesOptions(null));
        assertEquals("indicesOptions must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.types((String[]) null));
        assertEquals("types must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> searchRequest.types((String) null));
        assertEquals("type must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.searchType((SearchType)null));
        assertEquals("searchType must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.source(null));
        assertEquals("source must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.scroll((TimeValue)null));
        assertEquals("keepAlive must not be null", e.getMessage());
    }

    public void testEqualsAndHashcode() throws IOException {
        SearchRequest firstSearchRequest = createSearchRequest();
        assertNotNull("search request is equal to null", firstSearchRequest);
        assertNotEquals("search request  is equal to incompatible type", firstSearchRequest, "");
        assertEquals("search request is not equal to self", firstSearchRequest, firstSearchRequest);
        assertEquals("same source builder's hashcode returns different values if called multiple times",
                firstSearchRequest.hashCode(), firstSearchRequest.hashCode());

        SearchRequest secondSearchRequest = copyRequest(firstSearchRequest);
        assertEquals("search request  is not equal to self", secondSearchRequest, secondSearchRequest);
        assertEquals("search request is not equal to its copy", firstSearchRequest, secondSearchRequest);
        assertEquals("search request is not symmetric", secondSearchRequest, firstSearchRequest);
        assertEquals("search request copy's hashcode is different from original hashcode",
                firstSearchRequest.hashCode(), secondSearchRequest.hashCode());

        SearchRequest thirdSearchRequest = copyRequest(secondSearchRequest);
        assertEquals("search request is not equal to self", thirdSearchRequest, thirdSearchRequest);
        assertEquals("search request is not equal to its copy", secondSearchRequest, thirdSearchRequest);
        assertEquals("search request copy's hashcode is different from original hashcode",
                secondSearchRequest.hashCode(), thirdSearchRequest.hashCode());
        assertEquals("equals is not transitive", firstSearchRequest, thirdSearchRequest);
        assertEquals("search request copy's hashcode is different from original hashcode",
                firstSearchRequest.hashCode(), thirdSearchRequest.hashCode());
        assertEquals("equals is not symmetric", thirdSearchRequest, secondSearchRequest);
        assertEquals("equals is not symmetric", thirdSearchRequest, firstSearchRequest);

        boolean changed = false;
        if (randomBoolean()) {
            secondSearchRequest.indices(generateRandomStringArray(10, 10, false, false));
            if (Arrays.equals(secondSearchRequest.indices(), firstSearchRequest.indices()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.indicesOptions(
                    IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
            if (secondSearchRequest.indicesOptions().equals(firstSearchRequest.indicesOptions()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.types(generateRandomStringArray(10, 10, false, false));
            if (Arrays.equals(secondSearchRequest.types(), firstSearchRequest.types()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.preference(randomAsciiOfLengthBetween(3, 10));
            if (secondSearchRequest.preference().equals(firstSearchRequest.preference()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.routing(randomAsciiOfLengthBetween(3, 10));
            if (secondSearchRequest.routing().equals(firstSearchRequest.routing()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.requestCache(randomBoolean());
            if (secondSearchRequest.requestCache().equals(firstSearchRequest.requestCache()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.scroll(randomPositiveTimeValue());
            if (secondSearchRequest.scroll().equals(firstSearchRequest.scroll()) == false) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.searchType(randomFrom(SearchType.values()));
            if (secondSearchRequest.searchType() != firstSearchRequest.searchType()) {
                changed = true;
            }
        }
        if (randomBoolean()) {
            secondSearchRequest.source(createSearchSourceBuilder());
            if (secondSearchRequest.source().equals(firstSearchRequest.source()) == false) {
                changed = true;
            }
        }

        if (changed) {
            assertNotEquals(firstSearchRequest, secondSearchRequest);
            assertNotEquals(firstSearchRequest.hashCode(), secondSearchRequest.hashCode());
        } else {
            assertEquals(firstSearchRequest, secondSearchRequest);
            assertEquals(firstSearchRequest.hashCode(), secondSearchRequest.hashCode());
        }
    }

    public static SearchRequest createSearchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        if (randomBoolean()) {
            searchRequest.indices(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.types(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAsciiOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.routing(randomAsciiOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.scroll(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.values()));
        }
        if (randomBoolean()) {
            searchRequest.source(createSearchSourceBuilder());
        }
        return searchRequest;
    }

    private static SearchRequest copyRequest(SearchRequest searchRequest) throws IOException {
        SearchRequest result = new SearchRequest();
        result.indices(searchRequest.indices());
        result.indicesOptions(searchRequest.indicesOptions());
        result.types(searchRequest.types());
        result.searchType(searchRequest.searchType());
        result.preference(searchRequest.preference());
        result.routing(searchRequest.routing());
        result.requestCache(searchRequest.requestCache());
        result.scroll(searchRequest.scroll());
        if (searchRequest.source() != null) {
            result.source(searchRequest.source());
        }
        return result;
    }
}
