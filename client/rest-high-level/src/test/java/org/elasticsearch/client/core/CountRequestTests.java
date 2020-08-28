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

package org.elasticsearch.client.core;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

// similar to SearchRequestTests as CountRequest inline several members (and functionality) from SearchRequest
// In RestCountAction the request body is parsed as QueryBuilder (top level query field),
// so that is why this is chosen as server side instance.
public class CountRequestTests extends AbstractRequestTestCase<CountRequest, QueryBuilder> {

    @Override
    protected CountRequest createClientTestInstance() {
        CountRequest countRequest = new CountRequest();
        // query is the only property that is serialized as xcontent:
        if (randomBoolean()) {
            countRequest.query(new MatchAllQueryBuilder());
        }
        return countRequest;
    }

    @Override
    protected QueryBuilder doParseToServerInstance(XContentParser parser) throws IOException {
        return RestActions.getQueryContent(parser);
    }

    @Override
    protected void assertInstances(QueryBuilder serverInstance, CountRequest clientTestInstance) {
        // query is the only property that is serialized as xcontent:
        assertThat(serverInstance, equalTo(clientTestInstance.query()));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    public void testIllegalArguments() {
        CountRequest countRequest = new CountRequest();
        assertNotNull(countRequest.indices());
        assertNotNull(countRequest.indicesOptions());
        assertNotNull(countRequest.types());

        NullPointerException e = expectThrows(NullPointerException.class, () -> countRequest.indices((String[]) null));
        assertEquals("indices must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> countRequest.indices((String) null));
        assertEquals("index must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.indicesOptions(null));
        assertEquals("indicesOptions must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.types((String[]) null));
        assertEquals("types must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> countRequest.types((String) null));
        assertEquals("type must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.source(null));
        assertEquals("source must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> countRequest.query(null));
        assertEquals("query must not be null", e.getMessage());
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createCountRequest(), CountRequestTests::copyRequest, this::mutate);
    }

    private CountRequest createCountRequest() {
        CountRequest countRequest = new CountRequest("index");
        if (randomBoolean()) {
            countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        } else {
            countRequest.query(new MatchQueryBuilder("num", 10));
        }
        return countRequest;
    }

    private CountRequest mutate(CountRequest countRequest) {
        CountRequest mutation = copyRequest(countRequest);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(countRequest.indices(), new String[]{randomAlphaOfLength(10)})));
        mutators.add(() -> mutation.indicesOptions(randomValueOtherThan(countRequest.indicesOptions(),
            () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()))));
        mutators.add(() -> mutation.types(ArrayUtils.concat(countRequest.types(), new String[]{randomAlphaOfLength(10)})));
        mutators.add(() -> mutation.preference(randomValueOtherThan(countRequest.preference(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.routing(randomValueOtherThan(countRequest.routing(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.terminateAfter(randomValueOtherThan(countRequest.terminateAfter(), () -> randomIntBetween(0, 10))));
        mutators.add(() -> mutation.minScore(randomValueOtherThan(countRequest.minScore(), () -> (float) randomIntBetween(0, 10))));
        mutators.add(() -> mutation.query(randomValueOtherThan(countRequest.query(),
            () -> new MatchQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4)))));
        randomFrom(mutators).run();
        return mutation;
    }

    private static CountRequest copyRequest(CountRequest countRequest) {
        CountRequest result = new CountRequest();
        result.indices(countRequest.indices());
        result.indicesOptions(countRequest.indicesOptions());
        result.types(countRequest.types());
        result.routing(countRequest.routing());
        result.preference(countRequest.preference());
        if (countRequest.query() != null) {
            result.query(countRequest.query());
        }
        result.terminateAfter(countRequest.terminateAfter());
        result.minScore(countRequest.minScore());
        return result;
    }
}
