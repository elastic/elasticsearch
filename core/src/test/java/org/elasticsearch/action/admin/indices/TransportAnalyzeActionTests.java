/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;

public class TransportAnalyzeActionTests extends ESTestCase {

    private IndexAnalyzers indexAnalyzers;
    private AnalysisRegistry registry;
    private Environment environment;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put("index.analysis.filter.wordDelimiter.type", "word_delimiter")
                .put("index.analysis.filter.wordDelimiter.split_on_numerics", false)
                .put("index.analysis.analyzer.custom_analyzer.tokenizer", "whitespace")
                .putArray("index.analysis.analyzer.custom_analyzer.filter", "lowercase", "wordDelimiter")
                .put("index.analysis.analyzer.custom_analyzer.tokenizer", "whitespace")
                .putArray("index.analysis.analyzer.custom_analyzer.filter", "lowercase", "wordDelimiter")
                .put("index.analysis.tokenizer.trigram.type", "ngram")
                .put("index.analysis.tokenizer.trigram.min_gram", 3)
                .put("index.analysis.tokenizer.trigram.max_gram", 3)
                .put("index.analysis.filter.synonym.type", "synonym")
                .putArray("index.analysis.filter.synonym.synonyms", "kimchy => shay")
                .put("index.analysis.filter.synonym.tokenizer", "trigram")
                .put("index.analysis.filter.synonym.min_gram", 3)
                .put("index.analysis.filter.synonym.max_gram", 3).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        environment = new Environment(settings);
        registry = new AnalysisModule(environment, emptyList()).getAnalysisRegistry();
        indexAnalyzers = registry.build(idxSettings);
    }

    public void testNoIndexAnalyzers() throws IOException {
        AnalyzeRequest request = new AnalyzeRequest();
        request.analyzer("standard");
        request.text("the quick brown fox");
        AnalyzeResponse analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, null, registry, environment);
        List<AnalyzeResponse.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(4, tokens.size());

        request.analyzer(null);
        request.tokenizer("whitespace");
        request.addTokenFilter("lowercase");
        request.addTokenFilter("word_delimiter");
        request.text("the qu1ck brown fox");
        analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, randomBoolean() ? indexAnalyzers : null, registry, environment);
        tokens = analyze.getTokens();
        assertEquals(6, tokens.size());
        assertEquals("qu", tokens.get(1).getTerm());
        assertEquals("1", tokens.get(2).getTerm());
        assertEquals("ck", tokens.get(3).getTerm());

        request.analyzer(null);
        request.tokenizer("whitespace");
        request.addCharFilter("html_strip");
        request.addTokenFilter("lowercase");
        request.addTokenFilter("word_delimiter");
        request.text("<p>the qu1ck brown fox</p>");
        analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, randomBoolean() ? indexAnalyzers : null, registry, environment);
        tokens = analyze.getTokens();
        assertEquals(6, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("qu", tokens.get(1).getTerm());
        assertEquals("1", tokens.get(2).getTerm());
        assertEquals("ck", tokens.get(3).getTerm());
        assertEquals("brown", tokens.get(4).getTerm());
        assertEquals("fox", tokens.get(5).getTerm());
    }

    public void testFillsAttributes() throws IOException {
        AnalyzeRequest request = new AnalyzeRequest();
        request.analyzer("standard");
        request.text("the 1 brown fox");
        AnalyzeResponse analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, null, registry, environment);
        List<AnalyzeResponse.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(4, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals(0, tokens.get(0).getStartOffset());
        assertEquals(3, tokens.get(0).getEndOffset());
        assertEquals(0, tokens.get(0).getPosition());
        assertEquals("<ALPHANUM>", tokens.get(0).getType());

        assertEquals("1", tokens.get(1).getTerm());
        assertEquals(4, tokens.get(1).getStartOffset());
        assertEquals(5, tokens.get(1).getEndOffset());
        assertEquals(1, tokens.get(1).getPosition());
        assertEquals("<NUM>", tokens.get(1).getType());

        assertEquals("brown", tokens.get(2).getTerm());
        assertEquals(6, tokens.get(2).getStartOffset());
        assertEquals(11, tokens.get(2).getEndOffset());
        assertEquals(2, tokens.get(2).getPosition());
        assertEquals("<ALPHANUM>", tokens.get(2).getType());

        assertEquals("fox", tokens.get(3).getTerm());
        assertEquals(12, tokens.get(3).getStartOffset());
        assertEquals(15, tokens.get(3).getEndOffset());
        assertEquals(3, tokens.get(3).getPosition());
        assertEquals("<ALPHANUM>", tokens.get(3).getType());
    }

    public void testWithIndexAnalyzers() throws IOException {

        AnalyzeRequest request = new AnalyzeRequest();
        request.analyzer("standard");
        request.text("the quick brown fox");
        request.analyzer("custom_analyzer");
        request.text("the qu1ck brown fox");
        AnalyzeResponse analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, indexAnalyzers, registry, environment);
        List<AnalyzeResponse.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(4, tokens.size());

        request.analyzer("whitespace");
        request.text("the qu1ck brown fox-dog");
        analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, indexAnalyzers, registry, environment);
        tokens = analyze.getTokens();
        assertEquals(4, tokens.size());

        request.analyzer("custom_analyzer");
        request.text("the qu1ck brown fox-dog");
        analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, indexAnalyzers, registry, environment);
        tokens = analyze.getTokens();
        assertEquals(5, tokens.size());

        request.analyzer(null);
        request.tokenizer("whitespace");
        request.addTokenFilter("lowercase");
        request.addTokenFilter("wordDelimiter");
        request.text("the qu1ck brown fox-dog");
        analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, indexAnalyzers, registry, environment);
        tokens = analyze.getTokens();
        assertEquals(5, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("qu1ck", tokens.get(1).getTerm());
        assertEquals("brown", tokens.get(2).getTerm());
        assertEquals("fox", tokens.get(3).getTerm());
        assertEquals("dog", tokens.get(4).getTerm());

        request.analyzer(null);
        request.tokenizer("trigram");
        request.addTokenFilter("synonym");
        request.text("kimchy");
        analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, indexAnalyzers, registry, environment);
        tokens = analyze.getTokens();
        assertEquals(2, tokens.size());
        assertEquals("sha", tokens.get(0).getTerm());
        assertEquals("hay", tokens.get(1).getTerm());
    }

    public void testGetIndexAnalyserWithoutIndexAnalyzers() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeRequest()
                    .analyzer("custom_analyzer")
                    .text("the qu1ck brown fox-dog"),
                AllFieldMapper.NAME, null, null, registry, environment));
        assertEquals(e.getMessage(), "failed to find global analyzer [custom_analyzer]");
    }

    public void testUnknown() throws IOException {
        boolean notGlobal = randomBoolean();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeRequest()
                    .analyzer("foobar")
                    .text("the qu1ck brown fox"),
                AllFieldMapper.NAME, null, notGlobal ? indexAnalyzers : null, registry, environment));
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find analyzer [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global analyzer [foobar]");
        }

        e = expectThrows(IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeRequest()
                    .tokenizer("foobar")
                    .text("the qu1ck brown fox"),
                AllFieldMapper.NAME, null, notGlobal ? indexAnalyzers : null, registry, environment));
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find tokenizer under [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global tokenizer under [foobar]");
        }

        e = expectThrows(IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeRequest()
                    .tokenizer("whitespace")
                    .addTokenFilter("foobar")
                    .text("the qu1ck brown fox"),
                AllFieldMapper.NAME, null, notGlobal ? indexAnalyzers : null, registry, environment));
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find token filter under [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global token filter under [foobar]");
        }

        e = expectThrows(IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeRequest()
                    .tokenizer("whitespace")
                    .addTokenFilter("lowercase")
                    .addCharFilter("foobar")
                    .text("the qu1ck brown fox"),
                AllFieldMapper.NAME, null, notGlobal ? indexAnalyzers : null, registry, environment));
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find char filter under [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global char filter under [foobar]");
        }
    }

    public void testNonPreBuildTokenFilter() throws IOException {
        AnalyzeRequest request = new AnalyzeRequest();
        request.tokenizer("whitespace");
        request.addTokenFilter("min_hash");
        request.text("the quick brown fox");
        AnalyzeResponse analyze = TransportAnalyzeAction.analyze(request, AllFieldMapper.NAME, null, indexAnalyzers, registry, environment);
        List<AnalyzeResponse.AnalyzeToken> tokens = analyze.getTokens();
        int default_hash_count = 1;
        int default_bucket_size = 512;
        int default_hash_set_size = 1;
        assertEquals(default_hash_count * default_bucket_size * default_hash_set_size, tokens.size());

    }
}
