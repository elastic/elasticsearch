/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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

    public void testRandomVersionSerialization() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        Version version = VersionUtils.randomVersion(random());
        if (version.before(Version.V_7_11_0) && searchRequest.source() != null) {
            // Versions before 7.11.0 don't support runtime mappings
            searchRequest.source().runtimeMappings(emptyMap());
        }
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new, version);
        if (version.before(Version.V_7_0_0)) {
            assertTrue(deserializedRequest.isCcsMinimizeRoundtrips());
        } else {
            assertEquals(searchRequest.isCcsMinimizeRoundtrips(), deserializedRequest.isCcsMinimizeRoundtrips());
        }
        if (version.before(Version.V_6_7_0)) {
            assertNull(deserializedRequest.getLocalClusterAlias());
            assertAbsoluteStartMillisIsCurrentTime(deserializedRequest);
            assertTrue(deserializedRequest.isFinalReduce());
        } else {
            assertEquals(searchRequest.getLocalClusterAlias(), deserializedRequest.getLocalClusterAlias());
            assertEquals(searchRequest.getAbsoluteStartMillis(), deserializedRequest.getAbsoluteStartMillis());
            assertEquals(searchRequest.isFinalReduce(), deserializedRequest.isFinalReduce());
        }
    }

    public void testReadFromPre6_7_0() throws IOException {
        String msg = "AAEBBWluZGV4AAAAAQACAAAA/////w8AAAAAAAAA/////w8AAAAAAAACAAAAAAABAAMCBAUBAAKABACAAQIAAA==";
        try (StreamInput in = StreamInput.wrap(Base64.getDecoder().decode(msg))) {
            in.setVersion(VersionUtils.randomVersionBetween(random(), Version.V_6_4_0, VersionUtils.getPreviousVersion(Version.V_6_7_0)));
            SearchRequest searchRequest = new SearchRequest(in);
            assertArrayEquals(new String[] { "index" }, searchRequest.indices());
            assertNull(searchRequest.getLocalClusterAlias());
            assertAbsoluteStartMillisIsCurrentTime(searchRequest);
            assertTrue(searchRequest.isCcsMinimizeRoundtrips());
            assertTrue(searchRequest.isFinalReduce());
        }
    }

    private static void assertAbsoluteStartMillisIsCurrentTime(SearchRequest searchRequest) {
        long before = System.currentTimeMillis();
        long absoluteStartMillis = searchRequest.getOrCreateAbsoluteStartMillis();
        long after = System.currentTimeMillis();
        assertThat(absoluteStartMillis, allOf(greaterThanOrEqualTo(before), lessThanOrEqualTo(after)));
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
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("id"))
            ).scroll(TimeValue.timeValueMillis(randomIntBetween(1, 100)));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [point in time] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // Minimum compatible shard node version with ccs_minimize_roundtrips
            SearchRequest searchRequest;
            boolean isMinCompatibleShardVersion = randomBoolean();
            if (isMinCompatibleShardVersion) {
                searchRequest = new SearchRequest(VersionUtils.randomVersion(random()));
            } else {
                searchRequest = new SearchRequest();
            }

            boolean shouldSetCcsMinimizeRoundtrips = randomBoolean();
            if (shouldSetCcsMinimizeRoundtrips) {
                searchRequest.setCcsMinimizeRoundtrips(true);
            }
            ActionRequestValidationException validationErrors = searchRequest.validate();

            if (isMinCompatibleShardVersion && shouldSetCcsMinimizeRoundtrips) {
                assertNotNull(validationErrors);
                assertEquals(1, validationErrors.validationErrors().size());
                assertEquals(
                    "[ccs_minimize_roundtrips] cannot be [true] when setting a minimum compatible shard version",
                    validationErrors.validationErrors().get(0)
                );
            } else {
                assertNull(validationErrors);
            }
        }
        {
            // "enable_fields_emulation" should only work for qtf search type
            SearchRequest searchRequest = new SearchRequest();
            if (randomBoolean()) {
                // this should already be the default, but also randomly explicitely set it
                searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
            }
            searchRequest.setFieldsOptionEmulationEnabled(true);
            assertNull(searchRequest.validate());

            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals(
                "[enable_fields_emulation] cannot be used with [dfs_query_then_fetch] search type. Use the default "
                    + "[query_then_fetch] search type instead.",
                validationErrors.validationErrors().get(0)
            );
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
        SearchRequest searchRequest = createSearchRequest();
        // test fields emulation flag here since its part of equals/hashcode but not serialized
        if (randomBoolean()) {
            searchRequest.setFieldsOptionEmulationEnabled(randomBoolean());
        }
        checkEqualsAndHashCode(searchRequest, SearchRequest::new, this::mutate);
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
        mutators.add(() -> mutation.types(ArrayUtils.concat(searchRequest.types(), new String[] { randomAlphaOfLength(10) })));
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
        mutators.add(() -> mutation.setFieldsOptionEmulationEnabled(searchRequest.isFieldsOptionEmulationEnabled() == false));
        randomFrom(mutators).run();
        return mutation;
    }

    public void testDescriptionForDefault() {
        assertThat(toDescription(new SearchRequest()), equalTo("indices[], types[], search_type[QUERY_THEN_FETCH], source[]"));
    }

    public void testDescriptionIncludesScroll() {
        assertThat(
            toDescription(new SearchRequest().scroll(TimeValue.timeValueMinutes(5))),
            equalTo("indices[], types[], search_type[QUERY_THEN_FETCH], scroll[5m], source[]")
        );
    }

    public void testDescriptionIncludePreferenceAndRouting() {
        assertThat(
            toDescription(new SearchRequest().preference("abc")),
            equalTo("indices[], types[], search_type[QUERY_THEN_FETCH], source[], preference[abc]")
        );
        assertThat(
            toDescription(new SearchRequest().preference("abc").routing("xyz")),
            equalTo("indices[], types[], search_type[QUERY_THEN_FETCH], source[], routing[xyz], preference[abc]")
        );
    }

    private String toDescription(SearchRequest request) {
        return request.createTask(0, "test", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap()).getDescription();
    }
}
