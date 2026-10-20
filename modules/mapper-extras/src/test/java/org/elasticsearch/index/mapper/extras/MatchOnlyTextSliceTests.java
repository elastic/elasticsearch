/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.SliceTermsVerifier;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * A {@code match_only_text} field indexes terms without their positions and confirms a position against the value in
 * {@code _source}. On a slice index the term in the dictionary carries the slice and the value does not, so the one
 * term such a query carries cannot serve both.
 */
public class MatchOnlyTextSliceTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    private MapperService sliceMapperService() throws IOException {
        return createMapperService(
            Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build(),
            mapping(b -> b.startObject("body").field("type", "match_only_text").endObject())
        );
    }

    private SearchExecutionContext scopedTo(MapperService mapperService, String slice) {
        final SearchExecutionContext context = createSearchExecutionContext(mapperService);
        context.setSliceSelection(SliceSelection.fromSearchSlice(slice));
        return context;
    }

    public void testAPhraseIsDeclined() throws IOException {
        final SearchExecutionContext context = scopedTo(sliceMapperService(), "tenant-a");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MatchPhraseQueryBuilder("body", "quick brown").toQuery(context)
        );
        assertThat(e.getMessage(), containsString("queries over positions on field [body]"));
    }

    public void testAnIntervalIsDeclined() throws IOException {
        final SearchExecutionContext context = scopedTo(sliceMapperService(), "tenant-a");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new IntervalQueryBuilder("body", new IntervalsSourceProvider.Match("quick brown", 0, true, null, null, null)).toQuery(
                context
            )
        );
        assertThat(e.getMessage(), containsString("intervals on field [body]"));
    }

    /** A term names one term, which the field looks up under the slice like any other text field. */
    public void testATermIsAskedForUnderItsSlice() throws IOException {
        final MapperService mapperService = sliceMapperService();
        withLuceneIndex(mapperService, writer -> {
            // Only one slice holds the word, so a query that lost the slice would match the other one too.
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("body", "quick"), "tenant-a")).rootDoc());
            writer.addDocument(mapperService.documentMapper().parse(source("2", b -> b.field("body", "slow"), "tenant-b")).rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                final SliceSelection selection = SliceSelection.fromSearchSlice(slice);
                final IndexSearcher searcher = new IndexSearcher(SliceTermsVerifier.wrap(reader, selection));
                searcher.setQueryCache(null);
                final SearchExecutionContext context = scopedTo(mapperService, slice);
                assertEquals(slice.equals("tenant-a") ? 1 : 0, searcher.count(context.getFieldType("body").termQuery("quick", context)));
            }
        });
    }
}
