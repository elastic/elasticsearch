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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class SearchRequestTests extends AbstractSearchTestCase {

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

    public void testValidate() throws IOException {

        {
            // if scroll isn't set, validate should never add errors
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.scroll((Scroll) null);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNull(validationErrors);
        }
        {
            // disabling `track_total_hits` isn't valid in scroll context
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            // make sure we don't set the request cache for a scroll query
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            searchRequest.source().trackTotalHits(false);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("disabling [track_total_hits] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // scroll and `from` isn't valid
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            // make sure we don't set the request cache for a scroll query
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            searchRequest.source().from(10);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [from] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        checkEqualsAndHashCode(createSearchRequest(), SearchRequestTests::copyRequest, this::mutate);
    }

    private SearchRequest mutate(SearchRequest searchRequest) throws IOException {
        SearchRequest mutation = copyRequest(searchRequest);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(searchRequest.indices(), new String[] { randomAlphaOfLength(10) })));
        mutators.add(() -> mutation.indicesOptions(randomValueOtherThan(searchRequest.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()))));
        mutators.add(() -> mutation.types(ArrayUtils.concat(searchRequest.types(), new String[] { randomAlphaOfLength(10) })));
        mutators.add(() -> mutation.preference(randomValueOtherThan(searchRequest.preference(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.routing(randomValueOtherThan(searchRequest.routing(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.requestCache((randomValueOtherThan(searchRequest.requestCache(), () -> randomBoolean()))));
        mutators.add(() -> mutation
                .scroll(randomValueOtherThan(searchRequest.scroll(), () -> new Scroll(new TimeValue(randomNonNegativeLong() % 100000)))));
        mutators.add(() -> mutation.searchType(randomValueOtherThan(searchRequest.searchType(),
            () -> randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH))));
        mutators.add(() -> mutation.source(randomValueOtherThan(searchRequest.source(), this::createSearchSourceBuilder)));
        randomFrom(mutators).run();
        return mutation;
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
