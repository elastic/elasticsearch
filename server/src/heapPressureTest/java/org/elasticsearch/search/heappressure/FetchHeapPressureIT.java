/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * The fetch phase on the data node builds every hit of a shard before it sends any back, so everything a hit carries must be charged to the
 * request breaker as it is built. Each case asks for one kind of payload for every document, more than fits the heap, so it passes only if
 * that kind is charged.
 *
 * <p>The index has a single shard, so the whole fetch runs on one node, which also coordinates.
 */
public class FetchHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "fetch";
    private static final String NESTED_INDEX = "fetch-nested";
    /** Words of the {@code big} text, each matching the highlighted query. */
    private static final String WORD = "x".repeat(199);

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexLargeDocuments() throws IOException {
        String words = (WORD + " ").repeat(DOC_BYTES / (WORD.length() + 1));
        indexLargeDocuments(INDEX, 1, """
            {"type": "text", "store": true}""", words + "x".repeat(DOC_BYTES - words.length()));
    }

    /** The same documents, with {@code big} in a nested object. */
    @Before
    public void indexLargeNestedDocuments() throws IOException {
        String big = "x".repeat(DOC_BYTES);
        indexDocuments(
            NESTED_INDEX,
            1,
            Strings.format("""
                {"tag": {"type": "keyword"}, "items": {"type": "nested", "properties": {"big": %s}}}""", SOURCE_ONLY_KEYWORD),
            LARGE_DOCS,
            i -> Strings.format("""
                {"tag":"tag-%04d","items":[{"big":"%s"}]}""", i, big)
        );
    }

    public void testSourcesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(size -> search(INDEX, Strings.format("""
            {"size": %d}
            """, size)), 10, LARGE_DOCS, "fetch[source]");
    }

    /** {@code fields} copies the value out of {@code _source} into each hit. The source is not returned, so it is not what trips. */
    public void testFieldValuesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(size -> search(INDEX, Strings.format("""
            {"size": %d, "_source": false, "fields": ["big"]}
            """, size)), 10, LARGE_DOCS, "fetch[hit_fields]");
    }

    /** Stored fields come from the stored fields of the document, not from {@code _source}. */
    public void testStoredFieldsTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(size -> search(INDEX, Strings.format("""
            {"size": %d, "_source": false, "stored_fields": ["big"]}
            """, size)), 10, LARGE_DOCS, "fetch[hit_fields]");
    }

    /**
     * Every word matches and {@code number_of_fragments: 0} returns the whole field, so each highlighted hit is about the size of its
     * source.
     */
    public void testHighlightsTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(size -> search(INDEX, highlightEveryWord(size)), 10, LARGE_DOCS, "fetch[hit_fields]");
    }

    /**
     * Each hit fetches its nested documents, charging their sources on top of the hits before it. The outer source is not returned, so the
     * nested ones are the only payload.
     */
    public void testNestedInnerHitsTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(size -> search(NESTED_INDEX, nestedInnerHits(size)), 10, LARGE_DOCS, "fetch[source]");
    }

    private static String nestedInnerHits(int size) {
        return Strings.format("""
            {
              "size": %d,
              "_source": false,
              "query": {"nested": {"path": "items", "query": {"match_all": {}}, "inner_hits": {}}}
            }
            """, size);
    }

    private static String highlightEveryWord(int size) {
        return Strings.format("""
            {
              "size": %d,
              "_source": false,
              "query": {"match": {"big": "%s"}},
              "highlight": {"fields": {"big": {"number_of_fragments": 0}}}
            }
            """, size, WORD);
    }
}
