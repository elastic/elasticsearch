/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A {@code rank_features} feature is a term with its weight as the term frequency, so on a slice index the slice goes
 * on the feature name. Both ways of asking go through the field type: a term query on the feature subfield, and the
 * {@code rank_feature} query.
 */
public class RankFeaturesSliceTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    public void testAFeatureIsAskedForUnderItsSlice() throws IOException {
        final MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build(),
            mapping(b -> b.startObject("rf").field("type", "rank_features").endObject())
        );
        withLuceneIndex(mapperService, writer -> {
            // Only one slice holds the feature, so a query that lost the slice would match the other one too.
            writer.addDocument(
                mapperService.documentMapper()
                    .parse(source("1", b -> b.startObject("rf").field("shared", 2.0).endObject(), "tenant-a"))
                    .rootDoc()
            );
            writer.addDocument(
                mapperService.documentMapper()
                    .parse(source("2", b -> b.startObject("rf").field("other", 2.0).endObject(), "tenant-b"))
                    .rootDoc()
            );
            writer.forceMerge(1);
        }, reader -> {
            assertEquals(1, count(mapperService, reader, "tenant-a", context -> context.getFieldType("rf").termQuery("shared", context)));
            assertEquals(0, count(mapperService, reader, "tenant-b", context -> context.getFieldType("rf").termQuery("shared", context)));

            final CheckedFunction<SearchExecutionContext, Query, IOException> rankFeature = context -> new RankFeatureQueryBuilder(
                "rf.shared",
                new RankFeatureQueryBuilder.ScoreFunction.Linear()
            ).toQuery(context);
            assertEquals(1, count(mapperService, reader, "tenant-a", rankFeature));
            assertEquals(0, count(mapperService, reader, "tenant-b", rankFeature));
        });
    }

    private SearchExecutionContext scopedTo(MapperService mapperService, String slice) {
        final SearchExecutionContext context = createSearchExecutionContext(mapperService);
        context.setSliceSelection(SliceSelection.fromSearchSlice(slice));
        return context;
    }

    /** Counts the matches of a query built for {@code slice}. */
    private int count(
        MapperService mapperService,
        DirectoryReader reader,
        String slice,
        CheckedFunction<SearchExecutionContext, Query, IOException> query
    ) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        return searcher.count(query.apply(scopedTo(mapperService, slice)));
    }
}
