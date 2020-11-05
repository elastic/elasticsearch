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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class RankFeatureFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return 10;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("positive_score_impact", b -> b.field("positive_score_impact", false));
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, ParseContext.Document fields) {
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertEquals("_feature", termQuery.getTerm().field());
        assertEquals("field", termQuery.getTerm().text());
        assertNotNull(fields.getField("_feature"));
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        //always searchable even if it uses TextSearchInfo.NONE
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
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
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "rank_feature");
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", 10)));
        IndexableField[] fields = doc1.rootDoc().getFields("_feature");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(FeatureField.class));
        FeatureField featureField1 = (FeatureField) fields[0];

        ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", 12)));
        FeatureField featureField2 = (FeatureField) doc2.rootDoc().getFields("_feature")[0];

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testNegativeScoreImpact() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "rank_feature").field("positive_score_impact", false))
        );

        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", 10)));
        IndexableField[] fields = doc1.rootDoc().getFields("_feature");
        assertEquals(1, fields.length);
        assertThat(fields[0], instanceOf(FeatureField.class));
        FeatureField featureField1 = (FeatureField) fields[0];

        ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", 12)));
        FeatureField featureField2 = (FeatureField) doc2.rootDoc().getFields("_feature")[0];

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 > freq2);
    }

    public void testRejectMultiValuedFields() throws MapperParsingException, IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "rank_feature").endObject();
            b.startObject("foo").startObject("properties");
            {
                b.startObject("field").field("type", "rank_feature").endObject();
            }
            b.endObject().endObject();
        }));

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("field", Arrays.asList(10, 20))))
        );
        assertEquals("[rank_feature] fields do not support indexing multiple values for the same field [field] in the same document",
                e.getCause().getMessage());

        e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().field("field", 10).endObject();
                b.startObject().field("field", 20).endObject();
            }
            b.endArray();
        })));
        assertEquals("[rank_feature] fields do not support indexing multiple values for the same field [foo.field] in the same document",
                e.getCause().getMessage());
    }
}
