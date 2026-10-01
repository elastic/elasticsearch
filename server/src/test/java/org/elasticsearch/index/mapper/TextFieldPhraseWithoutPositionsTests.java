/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.List;

/**
 * A {@code text} field in strict columnar mode indexing no positions still answers a phrase query, by reading its own
 * values back out of the column and confirming the positions there.
 */
public class TextFieldPhraseWithoutPositionsTests extends MapperServiceTestCase {

    private static final List<String> DOCS = List.of("the quick brown fox jumps", "brown the quick fox", "quick brown", "nothing to see");

    private MapperService columnarMapper(String indexOptions) throws IOException {
        return createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build(),
            mapping(b -> b.startObject("body").field("type", "text").field("index_options", indexOptions).endObject())
        );
    }

    public void testPhraseWithoutPositionsMatchesTheSameDocuments() throws IOException {
        final MapperService withPositions = columnarMapper("positions");
        final MapperService withoutPositions = columnarMapper("docs");

        assertEquals(
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
            withPositions.fieldType("body").getTextSearchInfo().luceneFieldType().indexOptions()
        );
        assertEquals(IndexOptions.DOCS, withoutPositions.fieldType("body").getTextSearchInfo().luceneFieldType().indexOptions());

        for (String phrase : List.of("quick brown", "brown fox", "the quick brown fox", "fox the")) {
            final List<Integer> expected = matching(withPositions, phrase);
            final List<Integer> actual = matching(withoutPositions, phrase);
            assertEquals("phrase [" + phrase + "]", expected, actual);
            logger.info("phrase [{}] matched docs {} with and without positions", phrase, expected);
        }
    }

    private List<Integer> matching(MapperService mapperService, String phrase) throws IOException {
        final List<Integer> hits = new java.util.ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("body", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final Query query = new MatchPhraseQueryBuilder("body", phrase).toQuery(context);
            final IndexSearcher searcher = newSearcher(reader);
            for (var doc : searcher.search(query, 10).scoreDocs) {
                hits.add(doc.doc);
            }
        });
        java.util.Collections.sort(hits);
        return hits;
    }
}
