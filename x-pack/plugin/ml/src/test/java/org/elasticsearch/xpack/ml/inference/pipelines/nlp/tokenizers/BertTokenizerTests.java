/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;

public class BertTokenizerTests extends ESTestCase {

    public void testTokenize() {
        BertTokenizer tokenizer = BertTokenizer.builder(Arrays.asList("Elastic", "##search", "fun")).build();

        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun", false);
        assertThat(tokenization.getTokens(), contains("Elastic", "##search", "fun"));
        assertArrayEquals(new int[] {0, 1, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1}, tokenization.getTokenMap());
    }

    public void testTokenizeAppendSpecialTokens() {
        BertTokenizer tokenizer = BertTokenizer.builder(Arrays.asList(
            "elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN)).build();

        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("elasticsearch fun", true);
        assertThat(tokenization.getTokens(), contains("[CLS]", "elastic", "##search", "fun", "[SEP]"));
        assertArrayEquals(new int[] {3, 0, 1, 2, 4}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {-1, 0, 0, 1, -1}, tokenization.getTokenMap());
    }

    public void testBertVocab() throws IOException {
        BertTokenizer tokenizer = BertTokenizer.builder(loadVocab()).setDoLowerCase(false).build();

        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Jim bought 300 shares of Acme Corp. in 2006", true);

        assertArrayEquals(new int[] {101, 3104, 3306, 3127, 6117, 1104, 138, 1665, 3263, 13619, 119, 1107, 1386, 102},
            tokenization.getTokenIds());
    }

    public void testNeverSplitTokens() {
        final String specialToken = "SP001";

        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", specialToken, BertTokenizer.UNKNOWN_TOKEN))
            .setNeverSplit(Collections.singleton(specialToken))
            .build();

        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch " + specialToken + " fun", false);
        assertThat(tokenization.getTokens(), contains("Elastic", "##search", specialToken, "fun"));
        assertArrayEquals(new int[] {0, 1, 3, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2}, tokenization.getTokenMap());
    }

    public void testDoLowerCase() {
        {
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN))
                .setDoLowerCase(false)
                .build();

            BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun", false);
            assertThat(tokenization.getTokens(), contains(BertTokenizer.UNKNOWN_TOKEN, "fun"));
            assertArrayEquals(new int[] {3, 2}, tokenization.getTokenIds());
            assertArrayEquals(new int[] {0, 1}, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun", false);
            assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        }

        {
            BertTokenizer tokenizer = BertTokenizer.builder(Arrays.asList("elastic", "##search", "fun"))
                .setDoLowerCase(true)
                .build();

            BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun", false);
            assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> loadVocab() throws IOException {

        String path = "/org/elasticsearch/xpack/core/ml/inference/pipeline_config.json";
        URL resource = getClass().getResource(path);
        if (resource == null) {
            throw new ElasticsearchException("Could not find resource stored at [" + path + "]");
        }
        try(XContentParser parser =
                XContentType.JSON.xContent().createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    getClass().getResourceAsStream(path))) {

            Map<String, Object> map = parser.map();
            assertNotNull(map.get("task_type"));
            assertNotNull(map.get("vocab"));

            return (List<String>)map.get("vocab");
        }
    }
}
