/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Drives the search paths that build their own terms rather than going through a field type: highlighting and the term
 * vectors API. {@link SliceTermsVerifier} is interposed under assertions, so a path that reached the terms dictionary
 * without the slice fails here with the field and term named rather than silently matching nothing.
 *
 * <p>It also covers the requests a slice index declines, which are the ones that read the dictionary with a term the
 * search never named.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class SliceSearchPathsIT extends ESIntegTestCase {

    private static final String INDEX = "slices";

    @Before
    public void setUpSliceIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put("index.number_of_shards", 1).put(IndexSettings.SLICE_ENABLED.getKey(), true)
            ).setMapping("body", "type=text,term_vector=with_positions_offsets", "kw", "type=keyword")
        );
        ensureGreen(INDEX);
        indexInSlice("a", "the quick brown fox jumps", "alpha");
        indexInSlice("b", "the quick red fox sleeps", "beta");
        indexInSlice("c", "a lazy brown dog waits", "gamma");
        refresh(INDEX);
    }

    private void indexInSlice(String slice, String body, String keyword) {
        client().index(new IndexRequest(INDEX).source(Map.of("body", body, "kw", keyword)).routing(slice).setRoutingFromSlice(true))
            .actionGet();
    }

    /** A highlighter re-analyses the text and matches it against the index. */
    public void testHighlightingOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("body", "brown")).highlighter(new HighlightBuilder().field("body"))
        );
        builder.request().searchSlice("a");
        SearchResponse response = builder.get();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            final var highlight = response.getHits().getHits()[0].getHighlightFields().get("body");
            assertNotNull("the highlighter produced no field", highlight);
            // The fragment must show the plain term, not the slice-prefixed one.
            assertThat(highlight.fragments()[0].string(), containsString("<em>brown</em>"));
        } finally {
            response.decRef();
        }
    }

    /** The fast vector highlighter reads the stored term vectors, which carry the slice. */
    public void testFastVectorHighlightingOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("body", "quick"))
                .highlighter(new HighlightBuilder().field("body").highlighterType("fvh"))
        );
        builder.request().searchSlice("a");
        SearchResponse response = builder.get();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            final var highlight = response.getHits().getHits()[0].getHighlightFields().get("body");
            assertNotNull("the fast vector highlighter produced no field", highlight);
            assertThat(highlight.fragments()[0].string(), containsString("<em>quick</em>"));
        } finally {
            response.decRef();
        }
    }

    /** The term vectors API hands terms back to the user, so they must come without the slice. */
    public void testTermVectorsApiOnASliceIndex() throws IOException {
        final var request = client().prepareTermVectors(INDEX, idInSlice("a")).setFieldStatistics(false);
        request.request().routing("a").setRoutingFromSlice(true);
        final var response = request.get();
        final var terms = response.getFields().terms("body");
        assertNotNull(terms);
        final var iterator = terms.iterator();
        final List<String> returned = new ArrayList<>();
        for (var term = iterator.next(); term != null; term = iterator.next()) {
            returned.add(term.utf8ToString());
        }
        // The terms of the document, as indexed, carry the slice; what comes back must be the plain ones.
        assertThat(returned, containsInAnyOrder("the", "quick", "brown", "fox", "jumps"));
    }

    /**
     * The statistics of a term are read off the terms dictionary, which holds a copy of the term per slice. They are the
     * ones of the slice the document is in, rather than missing because the plain term is not in the dictionary.
     */
    public void testTermVectorsApiReportsTheStatisticsOfTheSlice() throws IOException {
        final var request = client().prepareTermVectors(INDEX, idInSlice("a")).setTermStatistics(true);
        request.request().routing("a").setRoutingFromSlice(true);
        final var terms = request.get().getFields().terms("body");
        final var iterator = terms.iterator();
        final Map<String, Integer> docFrequencies = new HashMap<>();
        for (var term = iterator.next(); term != null; term = iterator.next()) {
            docFrequencies.put(term.utf8ToString(), iterator.docFreq());
        }
        // "brown" is in one document of this slice and one of another; "jumps" only in this one.
        assertThat(docFrequencies.get("brown"), equalTo(1));
        assertThat(docFrequencies.get("jumps"), equalTo(1));
        assertThat(docFrequencies.get("the"), equalTo(1));
    }

    /** An artificial document is analysed rather than read from the index, so its terms carry no slice to remove. */
    public void testTermVectorsApiOnAnArtificialDocument() throws IOException {
        final var request = client().prepareTermVectors(INDEX, "unused")
            .setDoc(XContentFactory.jsonBuilder().startObject().field("body", "quick brown fox").field("kw", "alpha").endObject());
        request.request().routing("a").setRoutingFromSlice(true);
        final var response = request.get();
        assertTrue(response.isArtificial());
        final List<String> body = new ArrayList<>();
        final var iterator = response.getFields().terms("body").iterator();
        for (var term = iterator.next(); term != null; term = iterator.next()) {
            body.add(term.utf8ToString());
        }
        assertThat(body, containsInAnyOrder("quick", "brown", "fox"));
        // A keyword is indexed by value rather than through an analyzer, so this is where a prefix would show up.
        final var keywords = response.getFields().terms("kw").iterator();
        assertThat(keywords.next().utf8ToString(), equalTo("alpha"));
    }

    private String idInSlice(String slice) {
        final var search = prepareSearch(INDEX).setQuery(QueryBuilders.matchAllQuery());
        search.request().searchSlice(slice);
        final var hits = search.get();
        try {
            return hits.getHits().getHits()[0].getId();
        } finally {
            hits.decRef();
        }
    }

    /** A phrase is confined to the queried slice, including the positions the fast vector highlighter reads back. */
    public void testHighlightingAPhraseOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchPhraseQuery("body", "quick brown"))
                .highlighter(new HighlightBuilder().field("body").highlighterType("fvh"))
        );
        builder.request().searchSlice("a");
        SearchResponse response = builder.get();
        try {
            // "quick brown" is a phrase of slice [a] only: slice [b] holds "quick red" and slice [c] "lazy brown".
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            final var highlight = response.getHits().getHits()[0].getHighlightFields().get("body");
            assertNotNull("the fast vector highlighter produced no field", highlight);
            // The highlighter merges the two adjacent terms of the phrase into one fragment.
            assertThat(highlight.fragments()[0].string(), containsString("<em>quick brown</em>"));
        } finally {
            response.decRef();
        }
    }

    /**
     * {@code more_like_this} analyses text into terms of its own rather than asking a field type for them, so a slice
     * index declines it.
     */
    public void testMoreLikeThisIsDeclinedOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setQuery(
            QueryBuilders.moreLikeThisQuery(new String[] { "body" }, new String[] { "quick brown fox" }, null).minTermFreq(1).minDocFreq(1)
        );
        builder.request().searchSlice("a");
        final var failure = expectThrows(SearchPhaseExecutionException.class, builder::get);
        assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            failure.guessRootCauses()[0].getMessage(),
            containsString("[more_like_this] is not supported on an index with [index.slice.enabled] set")
        );
    }

    /**
     * The suggesters query the terms dictionary themselves, so they see neither the slice filter nor the slice on the terms.
     * A slice index declines them rather than suggest out of another tenant's terms.
     */
    public void testSuggestIsDeclinedOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setSource(
            new SearchSourceBuilder().suggest(new SuggestBuilder().addSuggestion("s", new TermSuggestionBuilder("body").text("quik")))
        );
        builder.request().searchSlice("a");
        final var failure = expectThrows(SearchPhaseExecutionException.class, builder::get);
        assertThat(failure.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            failure.guessRootCauses()[0].getMessage(),
            containsString("[suggest] is not supported on an index with [index.slice.enabled] set")
        );
    }
}
