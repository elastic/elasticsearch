/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The unified highlighter re-runs the query against a short-lived reader per hit and highlighted field. Phrase queries on
 * {@code match_only_text} confirm matches against stored {@code _source} there, and must take it from the source provider
 * the fetch phase installs at that point, not from the shared, per-segment caching provider of the query phase.
 */
public class MatchOnlyTextSourceLookupTests extends MapperServiceTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    public void testPhraseMatchesReadSourceFromTheProviderInstalledAtExecution() throws IOException {
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject("message").field("type", "match_only_text").endObject())
        );
        withLuceneIndex(mapperService, iw -> {
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("message", "request returned status 401")));
            doc.updateSeqID(0, 0);
            doc.version().setLongValue(0);
            iw.addDocuments(doc.docs());
        }, reader -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService, new IndexSearcher(reader));
            SourceProvider source = context.createSourceProvider(null);

            List<LeafReaderContext> queryPhaseReads = new ArrayList<>();
            context.setLookupProviders(recording(source, queryPhaseReads), LeafFieldLookupProvider.fromStoredFields());
            Query query = new MatchPhraseQueryBuilder("message", "returned status").toQuery(context);

            List<LeafReaderContext> fetchPhaseReads = new ArrayList<>();
            context.setLookupProviders(recording(source, fetchPhaseReads), LeafFieldLookupProvider.fromStoredFields());

            LeafReader shortLived = shortLived(getOnlyLeafReader(reader));
            IndexSearcher searcher = new IndexSearcher(shortLived);
            searcher.setQueryCache(null);
            Matches matches = searcher.rewrite(query)
                .createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f)
                .matches(shortLived.getContext(), 0);

            assertNotNull(matches);
            assertThat(queryPhaseReads, empty());
            assertThat(fetchPhaseReads, not(empty()));
            assertThat(fetchPhaseReads, everyItem(sameInstance(shortLived.getContext())));
        });
    }

    private static SourceProvider recording(SourceProvider delegate, List<LeafReaderContext> reads) {
        return (ctx, doc) -> {
            reads.add(ctx);
            return delegate.getSource(ctx, doc);
        };
    }

    /**
     * A non-cacheable wrapper like the one the unified highlighter builds in {@code FieldOffsetStrategy}.
     */
    private static LeafReader shortLived(LeafReader segment) {
        return new FilterLeafReader(segment) {
            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
