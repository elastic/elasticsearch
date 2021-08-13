/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

        Exception e = expectThrows(NullPointerException.class, () -> countRequest.indices((String[]) null));
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

        e = expectThrows(IllegalArgumentException.class, () -> countRequest.terminateAfter(-(randomIntBetween(1, 100))));
        assertEquals("terminateAfter must be > 0", e.getMessage());
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
