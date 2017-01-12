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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test for {@link TokenSumFieldMapper}.
 */
public class TokenSumFieldMapperTests extends ESSingleNodeTestCase {
    public void testMerge() throws IOException {
        String stage1Mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("person")
                    .startObject("properties")
                        .startObject("ts")
                            .field("type", "token_sum")
                            .field("analyzer", "keyword")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper stage1 = mapperService.merge("person",
                new CompressedXContent(stage1Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        String stage2Mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("person")
                    .startObject("properties")
                        .startObject("ts")
                            .field("type", "token_sum")
                            .field("analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        DocumentMapper stage2 = mapperService.merge("person",
                new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        // previous mapper has not been modified
        assertThat(((TokenSumFieldMapper) stage1.mappers().smartNameFieldMapper("ts")).analyzer(), equalTo("keyword"));
        // but the new one has the change
        assertThat(((TokenSumFieldMapper) stage2.mappers().smartNameFieldMapper("ts")).analyzer(), equalTo("standard"));
    }

    public void testCalculateSum() throws IOException {
        Token t1 = new Token("100", 0, 3);
        Token t2 = new Token("200", 0, 3);
        Token t3 = new Token("300", 0, 3);
        Token[] tokens = new Token[] {t1, t2, t3};
        Collections.shuffle(Arrays.asList(tokens), random());
        final TokenStream tokenStream = new CannedTokenStream(0, 0, tokens);
        Analyzer analyzer = new Analyzer() {
            @Override
            public TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new MockTokenizer(), tokenStream);
            }
        };
        assertThat(TokenSumFieldMapper.calculateSum(analyzer, "", ""), equalTo(600));
    }

    public void testCalculateSumWithNotANumber() throws IOException {
        Token t1 = new Token("100", 0, 3);
        Token t2 = new Token("NaN", 0, 3);
        Token t3 = new Token("300", 0, 3);
        Token[] tokens = new Token[] {t1, t2, t3};
        Collections.shuffle(Arrays.asList(tokens), random());
        final TokenStream tokenStream = new CannedTokenStream(0, 0, tokens);
        Analyzer analyzer = new Analyzer() {
            @Override
            public TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new MockTokenizer(), tokenStream);
            }
        };
        assertThat(TokenSumFieldMapper.calculateSum(analyzer, "", ""), equalTo(-1));
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testEmptyName() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("")
                            .field("type", "token_sum")
                            .field("analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        // Empty name not allowed in index created after 5.0
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }
}
