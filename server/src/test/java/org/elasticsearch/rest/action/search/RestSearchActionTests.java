package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class RestSearchActionTests extends RestActionTestCase {
    public void testRestRequestQueryParamsParsing() throws IOException {
        String body = "{\"query\": {\"match_all\" : {}}}\n";

        SearchRequest searchRequest = new SearchRequest();
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/_search")
            .withParams(new HashMap<>() {{
                put("explain", "true");
                put("stored_fields", "author,title");
                put("docvalue_fields", "author,title");
                put("from", "0");
                put("size", "5");
                put("sort", "author:asc,title:desc");
                put("_source", "true");
                put("terminate_after", "1");
                put("stats", "merge,refresh");
                put("timeout", "10s");
                put("track_scores", "true");
                put("track_total_hits", "true");
                put("version", "true");
                put("seq_no_primary_term", "true");
            }})
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
        IntConsumer setSize = size -> searchRequest.source().size(size);

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), body)) {
            RestSearchAction.parseSearchRequest(searchRequest, restRequest, parser, setSize);
        }

        assertTrue(searchRequest.source().explain());
        assertEquals(List.of("author", "title"), searchRequest.source().storedFields().fieldNames());
        assertEquals(List.of("author", "title"), searchRequest.source().docValueFields()
                                                                        .stream().map(f -> f.field).collect(Collectors.toList()));
        assertEquals(0, searchRequest.source().from());
        assertEquals(5, searchRequest.source().size());
        assertEquals(List.of(SortBuilders.fieldSort("author").order(SortOrder.ASC),
                                SortBuilders.fieldSort("title").order(SortOrder.DESC)),
                        searchRequest.source().sorts());
        assertTrue(searchRequest.source().fetchSource().fetchSource());
        assertEquals(1, searchRequest.source().terminateAfter());
        assertEquals(List.of("merge", "refresh"), searchRequest.source().stats());
        assertEquals(new TimeValue(10, TimeUnit.SECONDS), searchRequest.source().timeout());
        assertTrue(searchRequest.source().trackScores());
        assertEquals(SearchContext.TRACK_TOTAL_HITS_ACCURATE, searchRequest.source().trackTotalHitsUpTo().intValue());
        assertTrue(searchRequest.source().version());
        assertTrue(searchRequest.source().seqNoAndPrimaryTerm());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(singletonList(new NamedXContentRegistry.Entry(QueryBuilder.class,
            new ParseField(MatchAllQueryBuilder.NAME), (p, c) -> MatchAllQueryBuilder.fromXContent(p))));
    }
}
