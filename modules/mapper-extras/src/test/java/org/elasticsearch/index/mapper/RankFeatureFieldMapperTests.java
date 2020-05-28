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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class RankFeatureFieldMapperTests extends FieldMapperTestCase<RankFeatureFieldMapper.Builder> {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
        addModifier("positive_score_impact", false, (a, b) -> {
            a.fieldType().setPositiveScoreImpact(true);
            b.fieldType().setPositiveScoreImpact(false);
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    static int getFrequency(TokenStream tk) throws IOException {
        TermFrequencyAttribute freqAttribute = tk.addAttribute(TermFrequencyAttribute.class);
        tk.reset();
        assertTrue(tk.incrementToken());
        int freq = freqAttribute.getTermFrequency();
        assertFalse(tk.incrementToken());
        return freq;
    }

    @Override
    protected RankFeatureFieldMapper.Builder newBuilder() {
        return new RankFeatureFieldMapper.Builder("rank-feature");
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "rank_feature").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 10)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc1.rootDoc().getFields("_feature");
        assertEquals(1, fields.length);
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = (FeatureField) fields[0];

        ParsedDocument doc2 = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 12)
                        .endObject()),
                XContentType.JSON));

        FeatureField featureField2 = (FeatureField) doc2.rootDoc().getFields("_feature")[0];

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testNegativeScoreImpact() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "rank_feature")
                .field("positive_score_impact", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 10)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc1.rootDoc().getFields("_feature");
        assertEquals(1, fields.length);
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = (FeatureField) fields[0];

        ParsedDocument doc2 = mapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 12)
                        .endObject()),
                XContentType.JSON));

        FeatureField featureField2 = (FeatureField) doc2.rootDoc().getFields("_feature")[0];

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 > freq2);
    }

    public void testRejectMultiValuedFields() throws MapperParsingException, IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "rank_feature").endObject().startObject("foo")
                .startObject("properties").startObject("field").field("type", "rank_feature").endObject().endObject()
                .endObject().endObject().endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", BytesReference
                        .bytes(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("field", Arrays.asList(10, 20))
                                .endObject()),
                        XContentType.JSON)));
        assertEquals("[rank_feature] fields do not support indexing multiple values for the same field [field] in the same document",
                e.getCause().getMessage());

        e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", BytesReference
                        .bytes(XContentFactory.jsonBuilder()
                                .startObject()
                                    .startArray("foo")
                                        .startObject()
                                            .field("field", 10)
                                        .endObject()
                                        .startObject()
                                            .field("field", 20)
                                        .endObject()
                                    .endArray()
                                .endObject()),
                        XContentType.JSON)));
        assertEquals("[rank_feature] fields do not support indexing multiple values for the same field [foo.field] in the same document",
                e.getCause().getMessage());
    }

}
