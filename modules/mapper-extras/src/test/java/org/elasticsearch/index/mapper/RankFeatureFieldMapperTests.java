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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

public class RankFeatureFieldMapperTests extends FieldMapperTestCase2<RankFeatureFieldMapper.Builder> {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value(10);
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "store", "doc_values", "index");
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
    protected RankFeatureFieldMapper.Builder newBuilder() {
        return new RankFeatureFieldMapper.Builder("rank-feature");
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "rank_feature");
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", 10)));
        IndexableField[] fields = doc1.rootDoc().getFields("_feature");
        assertEquals(1, fields.length);
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
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
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
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

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());
        RankFeatureFieldMapper mapper = new RankFeatureFieldMapper.Builder("field").build(context);

        assertEquals(List.of(3.14f), fetchSourceValue(mapper, 3.14));
        assertEquals(List.of(42.9f), fetchSourceValue(mapper, "42.9"));
    }
}
