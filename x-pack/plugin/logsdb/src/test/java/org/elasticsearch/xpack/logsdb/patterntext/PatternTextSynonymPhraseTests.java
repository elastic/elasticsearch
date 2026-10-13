/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code pattern_text} field indexes no positions of its own and answers a phrase by reading its values, which
 * carry them. A word a synonym replaces with several is asked for as a phrase of those, as it is of a field that
 * indexes positions.
 */
public class PatternTextSynonymPhraseTests extends MapperServiceTestCase {

    private static final List<String> DOCS = List.of("new york", "york new", "new jersey");

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new LogsDBPlugin(Settings.EMPTY));
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "synonym",
                new NamedAnalyzer("synonym", AnalyzerScope.INDEX, multiWordSynonym())
            )
        );
    }

    public void testAMultiWordSynonymIsMatchedAsAPhrase() throws IOException {
        final QueryBuilder query = new MatchQueryBuilder("field", "ny").analyzer("synonym");
        final List<Integer> indexingPositions = matching("text", query);
        final List<Integer> patternText = matching("pattern_text", query);
        assertEquals("only the value holding the two words in order", List.of(0), indexingPositions);
        assertEquals(query.toString(), indexingPositions, patternText);
    }

    /** Holds {@code new york} beside {@code ny}, as a multi word synonym does. */
    private static Analyzer multiWordSynonym() {
        final SynonymMap.Builder synonyms = new SynonymMap.Builder(true);
        synonyms.add(new CharsRef("ny"), SynonymMap.Builder.join(new String[] { "new", "york" }, new CharsRefBuilder()), true);
        final SynonymMap map;
        try {
            map = synonyms.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                final Tokenizer source = new StandardTokenizer();
                return new TokenStreamComponents(source, new SynonymGraphFilter(source, map, true));
            }
        };
    }

    private List<Integer> matching(String type, QueryBuilder query) throws IOException {
        final MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", type)));
        final List<Integer> hits = new ArrayList<>();
        withLuceneIndex(mapperService, iw -> {
            for (String doc : DOCS) {
                iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", doc))).rootDoc());
            }
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            final IndexSearcher searcher = newSearcher(reader);
            for (var hit : searcher.search(query.toQuery(context), 10).scoreDocs) {
                hits.add(hit.doc);
            }
            Collections.sort(hits);
        });
        return hits;
    }
}
