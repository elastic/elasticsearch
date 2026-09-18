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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A phrase over this field is confirmed by reading the field's values back and analysing them again, so the reader it
 * reads them with has to match the layout they were written in. Decoding one layout as another does not fail, it
 * returns other bytes, which a phrase query shows as a document that quietly does not match.
 */
public class MatchOnlyTextColumnarPhraseTests extends MapperServiceTestCase {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    public void testPhraseMatchesOnAColumn() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        assertPhraseMatches(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
                .build()
        );
    }

    /** The same, for a layout the codec does not write, so the two cannot drift apart unnoticed. */
    public void testPhraseMatchesWithoutTheCodec() throws IOException {
        assertPhraseMatches(Settings.EMPTY);
    }

    private void assertPhraseMatches(Settings settings) throws IOException {
        final MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject("field").field("type", "match_only_text").endObject())
        );
        withLuceneIndex(
            mapperService,
            iw -> iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", "quick brown fox"))).rootDoc()),
            reader -> {
                final SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(reader));
                final IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals("phrase", 1, searcher.count(new MatchPhraseQueryBuilder("field", "quick brown").toQuery(context)));
                assertEquals(
                    "phrase prefix",
                    1,
                    searcher.count(new org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder("field", "quick bro").toQuery(context))
                );
                assertEquals(
                    "intervals",
                    1,
                    searcher.count(
                        new org.elasticsearch.index.query.IntervalQueryBuilder(
                            "field",
                            new org.elasticsearch.index.query.IntervalsSourceProvider.Match("quick brown", 0, true, null, null, null)
                        ).toQuery(context)
                    )
                );
                assertEquals(
                    "a phrase that is not there",
                    0,
                    searcher.count(new MatchPhraseQueryBuilder("field", "brown quick").toQuery(context))
                );
            }
        );
    }
}
