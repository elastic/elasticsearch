/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.TestCompoundRetrieverBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

public class SearchRequestTests extends AbstractSearchTestCase {

    @Override
    protected SearchRequest createSearchRequest() throws IOException {
        SearchRequest request = super.createSearchRequest();
        if (randomBoolean()) {
            return request;
        }
        // clusterAlias and absoluteStartMillis do not have public getters/setters hence we randomize them only in this test specifically.
        return SearchRequest.subSearchRequest(
            new TaskId("node", 1),
            request,
            request.indices(),
            randomAlphaOfLengthBetween(5, 10),
            randomNonNegativeLong(),
            randomBoolean()
        );
    }

    public void testWithLocalReduction() {
        final TaskId taskId = new TaskId("n", 1);
        expectThrows(
            NullPointerException.class,
            () -> SearchRequest.subSearchRequest(taskId, null, Strings.EMPTY_ARRAY, "", 0, randomBoolean())
        );
        SearchRequest request = new SearchRequest();
        expectThrows(NullPointerException.class, () -> SearchRequest.subSearchRequest(taskId, request, null, "", 0, randomBoolean()));
        expectThrows(
            NullPointerException.class,
            () -> SearchRequest.subSearchRequest(taskId, request, new String[] { null }, "", 0, randomBoolean())
        );
        expectThrows(
            NullPointerException.class,
            () -> SearchRequest.subSearchRequest(taskId, request, Strings.EMPTY_ARRAY, null, 0, randomBoolean())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> SearchRequest.subSearchRequest(taskId, request, Strings.EMPTY_ARRAY, "", -1, randomBoolean())
        );
        SearchRequest searchRequest = SearchRequest.subSearchRequest(taskId, request, Strings.EMPTY_ARRAY, "", 0, randomBoolean());
        assertNull(searchRequest.validate());
    }

    public void testSerialization() throws Exception {
        SearchRequest searchRequest = createSearchRequest();
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new);
        assertEquals(deserializedRequest, searchRequest);
        assertEquals(deserializedRequest.hashCode(), searchRequest.hashCode());
        assertNotSame(deserializedRequest, searchRequest);
    }

    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA)  // this can be removed when the affected transport version constants are collapsed
    public void testSerializationConstants() throws Exception {
        SearchRequest searchRequest = createSearchRequest();

        // something serialized with previous version to remove, should read correctly with the reversion
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersionUtils.getPreviousVersion(TransportVersions.REMOVE_MIN_COMPATIBLE_SHARD_NODE));
            searchRequest.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                in.setTransportVersion(TransportVersions.REVERT_REMOVE_MIN_COMPATIBLE_SHARD_NODE);
                SearchRequest copiedRequest = new SearchRequest(in);
                assertEquals(copiedRequest, searchRequest);
                assertEquals(copiedRequest.hashCode(), searchRequest.hashCode());
            }
        }
    }

    public void testSerializationMultiKNN() throws Exception {
        SearchRequest searchRequest = createSearchRequest();
        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        } else {
            // tests version prior to 8.8 so remove possible rank builder
            searchRequest.source().rankBuilder(null);
            // tests version prior to 8_500_999 so remove possible multiple queries
            searchRequest.source().subSearches(new ArrayList<>());
        }
        searchRequest.source()
            .knnSearch(
                List.of(
                    new KnnSearchBuilder(randomAlphaOfLength(10), new float[] { 1, 2 }, 5, 10, randomBoolean() ? null : randomFloat()),
                    new KnnSearchBuilder(randomAlphaOfLength(10), new float[] { 4, 12, 41 }, 3, 5, randomBoolean() ? null : randomFloat())
                )
            );
        expectThrows(
            IllegalArgumentException.class,
            () -> copyWriteable(
                searchRequest,
                namedWriteableRegistry,
                SearchRequest::new,
                TransportVersionUtils.randomVersionBetween(random(), TransportVersions.V_8_4_0, TransportVersions.V_8_6_0)
            )
        );

        searchRequest.source()
            .knnSearch(
                List.of(new KnnSearchBuilder(randomAlphaOfLength(10), new float[] { 1, 2 }, 5, 10, randomBoolean() ? null : randomFloat()))
            );
        // Shouldn't throw because its just one KNN request
        copyWriteable(
            searchRequest,
            namedWriteableRegistry,
            SearchRequest::new,
            TransportVersionUtils.randomVersionBetween(random(), TransportVersions.V_8_4_0, TransportVersions.V_8_6_0)
        );
    }

    public void testRandomVersionSerialization() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        TransportVersion version = TransportVersionUtils.randomVersion(random());
        if (version.before(TransportVersions.V_7_11_0) && searchRequest.source() != null) {
            // Versions before 7.11.0 don't support runtime mappings
            searchRequest.source().runtimeMappings(emptyMap());
        }
        if (version.before(TransportVersions.V_8_4_0)) {
            // Versions before 8.4.0 don't support force_synthetic_source
            searchRequest.setForceSyntheticSource(false);
        }
        if (version.before(TransportVersions.V_8_7_0) && searchRequest.hasKnnSearch() && searchRequest.source().knnSearch().size() > 1) {
            // Versions before 8.7.0 don't support more than one KNN clause
            searchRequest.source().knnSearch(List.of(searchRequest.source().knnSearch().get(0)));
        }
        if (version.before(TransportVersions.V_8_8_0) && searchRequest.source() != null) {
            // Versions before 8.8 don't support rank
            searchRequest.source().rankBuilder(null);
        }
        if (version.before(TransportVersions.V_8_9_X) && searchRequest.source() != null) {
            // Versions before 8_500_999 don't support queries
            searchRequest.source().subSearches(new ArrayList<>());
        }
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new, version);
        assertEquals(searchRequest.isCcsMinimizeRoundtrips(), deserializedRequest.isCcsMinimizeRoundtrips());
        assertEquals(searchRequest.getLocalClusterAlias(), deserializedRequest.getLocalClusterAlias());
        assertEquals(searchRequest.getAbsoluteStartMillis(), deserializedRequest.getAbsoluteStartMillis());
        assertEquals(searchRequest.isFinalReduce(), deserializedRequest.isFinalReduce());
    }

    public void testIllegalArguments() {
        SearchRequest searchRequest = new SearchRequest();
        assertNotNull(searchRequest.indices());
        assertNotNull(searchRequest.indicesOptions());
        assertNotNull(searchRequest.searchType());

        NullPointerException e = expectThrows(NullPointerException.class, () -> searchRequest.indices((String[]) null));
        assertEquals("indices must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> searchRequest.indices((String) null));
        assertEquals("index must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.indicesOptions(null));
        assertEquals("indicesOptions must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.searchType((SearchType) null));
        assertEquals("searchType must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.source(null));
        assertEquals("source must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.scroll((TimeValue) null));
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
        {
            // scroll and `size` is `0`
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder().size(0));
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[size] cannot be [0] in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // scroll and search_after
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            searchRequest.source().searchAfter(new String[] { "value" });
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[search_after] cannot be used in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // scroll and collapse
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            searchRequest.source().collapse(new CollapseBuilder("field"));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("cannot use `collapse` in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // allow_partial_results and compound retriever
            SearchRequest searchRequest = createSearchRequest().source(
                new SearchSourceBuilder().retriever(new TestCompoundRetrieverBuilder(randomIntBetween(1, 10)))
            );
            searchRequest.allowPartialSearchResults(true);
            searchRequest.scroll((Scroll) null);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals(
                "cannot specify [test_compound_retriever_builder] and [allow_partial_search_results]",
                validationErrors.validationErrors().get(0)
            );
        }
        {
            // scroll and compound retriever
            SearchRequest searchRequest = createSearchRequest().source(
                new SearchSourceBuilder().retriever(new TestCompoundRetrieverBuilder(randomIntBetween(1, 10)))
            );
            searchRequest.allowPartialSearchResults(false);
            searchRequest.scroll(TimeValue.timeValueMinutes(1));
            searchRequest.requestCache(false);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("cannot specify [test_compound_retriever_builder] and [scroll]", validationErrors.validationErrors().get(0));
        }
        {
            // allow_partial_results and non-compound retriever
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder().retriever(new RetrieverBuilder() {
                @Override
                public QueryBuilder topDocsQuery() {
                    return null;
                }

                @Override
                public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                    // no-op
                }

                @Override
                public String getName() {
                    return "not_a_compound_retriever";
                }

                @Override
                protected void doToXContent(XContentBuilder builder, Params params) throws IOException {}

                @Override
                protected boolean doEquals(Object o) {
                    return false;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }
            }));
            searchRequest.allowPartialSearchResults(true);
            searchRequest.scroll((Scroll) null);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNull(validationErrors);
        }
        {
            // allow_partial_results not defined and compound retriever
            SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().retriever(new RetrieverBuilder() {
                @Override
                public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
                    // no-op
                }

                @Override
                public String getName() {
                    return "compound_retriever";
                }

                @Override
                protected void doToXContent(XContentBuilder builder, Params params) throws IOException {}

                @Override
                protected boolean doEquals(Object o) {
                    return false;
                }

                @Override
                protected int doHashCode() {
                    return 0;
                }

                @Override
                public boolean isCompound() {
                    return true;
                }

                @Override
                public QueryBuilder topDocsQuery() {
                    return null;
                }
            }));
            searchRequest.scroll((Scroll) null);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNull(validationErrors);
        }
        {
            // search_after and `from` isn't valid
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.scroll((Scroll) null);
            searchRequest.source().searchAfter(new String[] { "value" });
            searchRequest.source().from(10);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[from] parameter must be set to 0 when [search_after] is used", validationErrors.validationErrors().get(0));
        }
        {
            // slice without scroll or pit
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.scroll((Scroll) null);
            searchRequest.source().pointInTimeBuilder(null);
            searchRequest.source().slice(new SliceBuilder(1, 10));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[slice] can only be used with [scroll] or [point-in-time] requests", validationErrors.validationErrors().get(0));
        }
        {
            // stored fields disabled with _source requested
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.scroll((Scroll) null);
            searchRequest.source().storedField("_none_");
            searchRequest.source().fetchSource(true);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[stored_fields] cannot be disabled if [_source] is requested", validationErrors.validationErrors().get(0));
        }
        {
            // stored fields disabled with fetch fields requested
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.scroll((Scroll) null);
            searchRequest.source().storedField("_none_");
            searchRequest.source().fetchSource(false);
            searchRequest.source().fetchField("field");
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[stored_fields] cannot be disabled when using the [fields] option", validationErrors.validationErrors().get(0));
        }
        {
            // cannot have multiple queries without rank
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            if (searchRequest.scroll() != null) {
                searchRequest.requestCache(false);
            }
            searchRequest.source()
                .subSearches(
                    List.of(
                        new SubSearchSourceBuilder(new TermQueryBuilder("three", "four")),
                        new SubSearchSourceBuilder(new TermQueryBuilder("five", "six"))
                    )
                );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[sub_searches] requires [rank]", validationErrors.validationErrors().get(0));
        }
        {
            // Rescore is not allowed on scroll requests
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.source().addRescorer(new QueryRescorerBuilder(QueryBuilders.matchAllQuery()));
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [rescore] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // Reader context with scroll
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(
                    new PointInTimeBuilder(new BytesArray(Base64.getUrlEncoder().encode("id".getBytes(StandardCharsets.UTF_8))))
                )
            ).scroll(TimeValue.timeValueMillis(randomIntBetween(1, 100)));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [point in time] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(100))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
                    .size(0)
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[rank] requires [size] greater than [0]", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(1))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
                    .size(2)
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals(
                "[rank] requires [rank_window_size: 1] be greater than or equal to [size: 2]",
                validationErrors.validationErrors().get(0)
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(100)).query(QueryBuilders.termQuery("field", "term"))
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(validationErrors.validationErrors().toString(), 1, validationErrors.validationErrors().size());
            assertEquals(
                "[rank] requires a minimum of [2] result sets using a combination of sub searches and/or knn searches",
                validationErrors.validationErrors().get(0)
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(100))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
            ).scroll(new TimeValue(1000));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[rank] cannot be used in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(9))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals(
                "[rank] requires [rank_window_size: 9] be greater than or equal to [size: 10]",
                validationErrors.validationErrors().get(0)
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(3))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
                    .size(3)
                    .from(4)
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNull(validationErrors);
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(100))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
                    .addRescorer(new QueryRescorerBuilder(QueryBuilders.termQuery("rescore", "another term")))
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[rank] cannot be used with [rescore]", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().rankBuilder(new TestRankBuilder(100))
                    .query(QueryBuilders.termQuery("field", "term"))
                    .knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 0f }, 10, 100, null)))
                    .suggest(new SuggestBuilder().setGlobalText("test").addSuggestion("suggestion", new TermSuggestionBuilder("term")))
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[rank] cannot be used with [suggest]", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY))
            );
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals(
                "[indices] cannot be used with point in time. Do not specify any index with point in time.",
                validationErrors.validationErrors().get(0)
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED)
                .source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY)));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[indicesOptions] cannot be used with point in time", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest().routing("route1")
                .source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY)));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[routing] cannot be used with point in time", validationErrors.validationErrors().get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest().preference("pref1")
                .source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY)));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[preference] cannot be used with point in time", validationErrors.validationErrors().get(0));
        }
    }

    public void testCopyConstructor() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new);
        assertEquals(deserializedRequest, searchRequest);
        assertEquals(deserializedRequest.hashCode(), searchRequest.hashCode());
        assertNotSame(deserializedRequest, searchRequest);
    }

    public void testEqualsAndHashcode() throws IOException {
        checkEqualsAndHashCode(createSearchRequest(), SearchRequest::new, this::mutate);
    }

    private SearchRequest mutate(SearchRequest searchRequest) {
        SearchRequest mutation = new SearchRequest(searchRequest);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(searchRequest.indices(), new String[] { randomAlphaOfLength(10) })));
        mutators.add(
            () -> mutation.indicesOptions(
                randomValueOtherThan(
                    searchRequest.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                )
            )
        );
        mutators.add(() -> mutation.preference(randomValueOtherThan(searchRequest.preference(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.routing(randomValueOtherThan(searchRequest.routing(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.requestCache((randomValueOtherThan(searchRequest.requestCache(), ESTestCase::randomBoolean))));
        mutators.add(
            () -> mutation.scroll(
                randomValueOtherThan(searchRequest.scroll(), () -> new Scroll(new TimeValue(randomNonNegativeLong() % 100000)))
            )
        );
        mutators.add(
            () -> mutation.searchType(
                randomValueOtherThan(
                    searchRequest.searchType(),
                    () -> randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH)
                )
            )
        );
        mutators.add(() -> mutation.source(randomValueOtherThan(searchRequest.source(), this::createSearchSourceBuilder)));
        mutators.add(() -> mutation.setCcsMinimizeRoundtrips(searchRequest.isCcsMinimizeRoundtrips() == false));
        randomFrom(mutators).run();
        return mutation;
    }

    public void testDescriptionForDefault() {
        assertThat(toDescription(new SearchRequest()), equalTo("indices[], search_type[QUERY_THEN_FETCH], source[]"));
    }

    public void testDescriptionIncludesScroll() {
        assertThat(
            toDescription(new SearchRequest().scroll(TimeValue.timeValueMinutes(5))),
            equalTo("indices[], search_type[QUERY_THEN_FETCH], scroll[5m], source[]")
        );
    }

    public void testDescriptionIncludePreferenceAndRouting() {
        assertThat(
            toDescription(new SearchRequest().preference("abc")),
            equalTo("indices[], search_type[QUERY_THEN_FETCH], source[], preference[abc]")
        );
        assertThat(
            toDescription(new SearchRequest().preference("abc").routing("xyz")),
            equalTo("indices[], search_type[QUERY_THEN_FETCH], source[], routing[xyz], preference[abc]")
        );
    }

    private String toDescription(SearchRequest request) {
        return request.createTask(0, "test", TransportSearchAction.TYPE.name(), TaskId.EMPTY_TASK_ID, emptyMap()).getDescription();
    }

    public void testForceSyntheticUnsupported() {
        SearchRequest request = new SearchRequest();
        request.setForceSyntheticSource(true);
        StreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersions.V_8_3_0);
        Exception e = expectThrows(IllegalArgumentException.class, () -> request.writeTo(out));
        assertEquals(e.getMessage(), "force_synthetic_source is not supported before 8.4.0");
    }
}
