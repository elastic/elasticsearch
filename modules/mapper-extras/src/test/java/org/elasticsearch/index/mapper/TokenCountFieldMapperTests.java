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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
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
 * Test for {@link TokenCountFieldMapper}.
 */
public class TokenCountFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, MapperExtrasPlugin.class);
    }

    public void testMerge() throws IOException {
        String stage1Mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("person")
                    .startObject("properties")
                        .startObject("tc")
                            .field("type", "token_count")
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
                        .startObject("tc")
                            .field("type", "token_count")
                            .field("analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        DocumentMapper stage2 = mapperService.merge("person",
                new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        // previous mapper has not been modified
        assertThat(((TokenCountFieldMapper) stage1.mappers().smartNameFieldMapper("tc")).analyzer(), equalTo("keyword"));
        // but the new one has the change
        assertThat(((TokenCountFieldMapper) stage2.mappers().smartNameFieldMapper("tc")).analyzer(), equalTo("standard"));
    }

    /**
     *  When position increments are counted, we're looking to make sure that we:
        - don't count tokens without an increment
        - count normal tokens with one increment
        - count funny tokens with more than one increment
        - count the final token increments on the rare token streams that have them
     */
    public void testCountPositionsWithIncrements() throws IOException {
        Analyzer analyzer = createMockAnalyzer();
        assertThat(TokenCountFieldMapper.countPositions(analyzer, "", "", true), equalTo(7));
    }

    /**
     *  When position increments are not counted (only positions are counted), we're looking to make sure that we:
        - don't count tokens without an increment
        - count normal tokens with one increment
        - count funny tokens with more than one increment as only one
        - don't count the final token increments on the rare token streams that have them
     */
    public void testCountPositionsWithoutIncrements() throws IOException {
        Analyzer analyzer = createMockAnalyzer();
        assertThat(TokenCountFieldMapper.countPositions(analyzer, "", "", false), equalTo(2));
    }

    private Analyzer createMockAnalyzer() {
        Token t1 = new Token();      // Token without an increment
        t1.setPositionIncrement(0);
        Token t2 = new Token();
        t2.setPositionIncrement(1);  // Normal token with one increment
        Token t3 = new Token();
        t2.setPositionIncrement(2);  // Funny token with more than one increment
        int finalTokenIncrement = 4; // Final token increment
        Token[] tokens = new Token[] {t1, t2, t3};
        Collections.shuffle(Arrays.asList(tokens), random());
        final TokenStream tokenStream = new CannedTokenStream(finalTokenIncrement, 0, tokens);
        // TODO: we have no CannedAnalyzer?
        Analyzer analyzer = new Analyzer() {
            @Override
            public TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new MockTokenizer(), tokenStream);
            }
        };
        return analyzer;
    }

    public void testEmptyName() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("")
                            .field("type", "token_count")
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

    public void testParseNullValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField();
        ParseContext.Document doc = parseDocument(mapper, createDocument(null));
        assertNull(doc.getField("test.tc"));
    }

    public void testParseEmptyValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField();
        ParseContext.Document doc = parseDocument(mapper, createDocument(""));
        assertEquals(0, doc.getField("test.tc").numericValue());
    }

    public void testParseNotNullValue() throws Exception {
        DocumentMapper mapper = createIndexWithTokenCountField();
        ParseContext.Document doc = parseDocument(mapper, createDocument("three tokens string"));
        assertEquals(3, doc.getField("test.tc").numericValue());
    }

    private DocumentMapper createIndexWithTokenCountField() throws IOException {
        final String content = XContentFactory.jsonBuilder().startObject()
            .startObject("person")
                .startObject("properties")
                    .startObject("test")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("tc")
                                .field("type", "token_count")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().endObject().string();

        return createIndex("test").mapperService().documentMapperParser().parse("person", new CompressedXContent(content));
    }

    private SourceToParse createDocument(String fieldValue) throws Exception {
        BytesReference request = XContentFactory.jsonBuilder()
            .startObject()
                .field("test", fieldValue)
            .endObject().bytes();

        return SourceToParse.source("test", "person", "1", request, XContentType.JSON);
    }

    private ParseContext.Document parseDocument(DocumentMapper mapper, SourceToParse request) {
        return mapper.parse(request)
            .docs().stream().findFirst().orElseThrow(() -> new IllegalStateException("Test object not parsed"));
    }
}
