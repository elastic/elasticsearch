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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test for {@link LegacyTokenCountFieldMapper}.
 */
public class LegacyTokenCountFieldMapperTests extends ESSingleNodeTestCase {

    private static final Settings BW_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0).build();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
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
        MapperService mapperService = createIndex("test", BW_SETTINGS).mapperService();
        DocumentMapper stage1 = mapperService.merge("person", new CompressedXContent(stage1Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        String stage2Mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("person")
                    .startObject("properties")
                        .startObject("tc")
                            .field("type", "token_count")
                            .field("analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        DocumentMapper stage2 = mapperService.merge("person", new CompressedXContent(stage2Mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        // previous mapper has not been modified
        assertThat(((LegacyTokenCountFieldMapper) stage1.mappers().smartNameFieldMapper("tc")).analyzer(), equalTo("keyword"));
        // but the new one has the change
        assertThat(((LegacyTokenCountFieldMapper) stage2.mappers().smartNameFieldMapper("tc")).analyzer(), equalTo("standard"));
    }

    public void testCountPositions() throws IOException {
        // We're looking to make sure that we:
        Token t1 = new Token();      // Don't count tokens without an increment
        t1.setPositionIncrement(0);
        Token t2 = new Token();
        t2.setPositionIncrement(1);  // Count normal tokens with one increment
        Token t3 = new Token();
        t2.setPositionIncrement(2);  // Count funny tokens with more than one increment
        int finalTokenIncrement = 4; // Count the final token increment on the rare token streams that have them
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
        assertThat(LegacyTokenCountFieldMapper.countPositions(analyzer, "", ""), equalTo(7));
    }
}
