/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PhraseSuggesterCircuitBreakerTests extends ESTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testCBReleasedAfterEachCollateIteration() throws IOException {
        Directory dir = new ByteBuffersDirectory();

        Map<String, Analyzer> analyzerMap = new HashMap<>();
        analyzerMap.put("body_ngram", new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                ShingleFilter sf = new ShingleFilter(t, 2, 3);
                sf.setOutputUnigrams(false);
                return new TokenStreamComponents(t, new LowerCaseFilter(sf));
            }
        });
        analyzerMap.put("body", new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer t = new StandardTokenizer();
                return new TokenStreamComponents(t, new LowerCaseFilter(t));
            }
        });
        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), analyzerMap);

        IndexWriterConfig conf = new IndexWriterConfig(wrapper);
        try (IndexWriter writer = new IndexWriter(dir, conf)) {
            for (String line : new String[] { "captain america", "american ace", "captain marvel", "american hero", "captain planet" }) {
                Document doc = new Document();
                doc.add(new Field("body", line, TextField.TYPE_NOT_STORED));
                doc.add(new Field("body_ngram", line, TextField.TYPE_NOT_STORED));
                writer.addDocument(doc);
            }
        }

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);

            IndexVersion indexVersion = IndexVersion.current();
            Settings indexSettingsSettings = indexSettings(indexVersion, 1, 1).build();
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("test").settings(indexSettingsSettings).build(),
                Settings.EMPTY
            );
            KeywordFieldMapper bodyMapper = new KeywordFieldMapper.Builder("body", indexSettings).build(
                MapperBuilderContext.root(false, false)
            );
            MappingLookup mappingLookup = MappingLookup.fromMappers(
                Mapping.EMPTY,
                List.of(bodyMapper),
                Collections.emptyList(),
                IndexMode.STANDARD
            );

            SearchExecutionContext baseCtx = new SearchExecutionContext(
                0,
                0,
                indexSettings,
                null,
                null,
                null,
                mappingLookup,
                null,
                null,
                parserConfig(),
                writableRegistry(),
                null,
                searcher,
                System::currentTimeMillis,
                null,
                null,
                () -> true,
                null,
                Collections.emptyMap(),
                null,
                MapperMetrics.NOOP
            );
            CircuitBreaker cb = newLimitedBreaker(ByteSizeValue.ofMb(100));
            SearchExecutionContext ctx = new SearchExecutionContext(baseCtx, cb);

            TemplateScript.Factory scriptFactory = params -> new TemplateScript(params) {
                @Override
                public String execute() {
                    return "{\"wildcard\":{\"body\":{\"value\":\"captain*\"}}}";
                }
            };

            PhraseSuggestionContext suggestion = new PhraseSuggestionContext(ctx);
            suggestion.setField("body_ngram");
            suggestion.setAnalyzer(wrapper);
            suggestion.setSize(5);
            suggestion.setShardSize(5);
            suggestion.setGramSize(2);
            suggestion.setConfidence(0.0f);
            suggestion.setMaxErrors(2.0f);
            suggestion.setRequireUnigram(false);
            suggestion.setCollateQueryScript(scriptFactory);
            suggestion.setText(new BytesRef("captan amrica"));

            PhraseSuggestionContext.DirectCandidateGenerator generator = new PhraseSuggestionContext.DirectCandidateGenerator();
            generator.setField("body");
            generator.suggestMode(SuggestMode.SUGGEST_MORE_POPULAR);
            generator.size(10);
            generator.accuracy(0.3f);
            generator.minWordLength(2);
            suggestion.addGenerator(generator);

            assertEquals("CB must be zero before innerExecute", 0L, cb.getUsed());

            Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> result =
                PhraseSuggester.INSTANCE.innerExecute("test", suggestion, searcher, new CharsRefBuilder());

            assertNotNull("innerExecute must return a result", result);

            assertThat(
                "CB tracked bytes must be fully released after innerExecute (fix: release per correction)",
                ctx.getQueryConstructionMemoryUsed(),
                equalTo(0L)
            );
            assertThat("Raw CB usage must be zero after innerExecute", cb.getUsed(), equalTo(0L));
        }
    }

    public void testNoCBUsageWithoutCollateScript() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()))) {
            Document doc = new Document();
            doc.add(new Field("body", "hello world", TextField.TYPE_NOT_STORED));
            writer.addDocument(doc);
        }

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            CircuitBreaker cb = newLimitedBreaker(ByteSizeValue.ofMb(100));

            IndexVersion indexVersion = IndexVersion.current();
            Settings indexSettingsSettings = indexSettings(indexVersion, 1, 1).build();
            IndexSettings indexSettings = new IndexSettings(
                IndexMetadata.builder("test").settings(indexSettingsSettings).build(),
                Settings.EMPTY
            );
            SearchExecutionContext baseCtx = new SearchExecutionContext(
                0,
                0,
                indexSettings,
                null,
                null,
                null,
                MappingLookup.EMPTY,
                null,
                null,
                parserConfig(),
                writableRegistry(),
                null,
                searcher,
                System::currentTimeMillis,
                null,
                null,
                () -> true,
                null,
                Collections.emptyMap(),
                null,
                MapperMetrics.NOOP
            );
            SearchExecutionContext ctx = new SearchExecutionContext(baseCtx, cb);

            PhraseSuggestionContext suggestion = new PhraseSuggestionContext(ctx);
            suggestion.setField("body");
            suggestion.setAnalyzer(new WhitespaceAnalyzer());
            suggestion.setSize(5);
            suggestion.setShardSize(5);
            suggestion.setGramSize(1);
            suggestion.setConfidence(0.0f);
            suggestion.setText(new BytesRef("hello"));

            PhraseSuggester.INSTANCE.innerExecute("test", suggestion, searcher, new CharsRefBuilder());
            assertThat("No CB bytes should be used when there is no collate script", cb.getUsed(), equalTo(0L));
        }
    }
}
