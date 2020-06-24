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

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SimilarityTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testResolveDefaultSimilarities() {
        SimilarityService similarityService = createIndex("foo").similarityService();
        assertThat(similarityService.getSimilarity("BM25").get(), instanceOf(LegacyBM25Similarity.class));
        assertThat(similarityService.getSimilarity("boolean").get(), instanceOf(BooleanSimilarity.class));
        assertThat(similarityService.getSimilarity("default"), equalTo(null));
    }

    public void testResolveSimilaritiesFromMapping_bm25() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "BM25")
            .put("index.similarity.my_similarity.k1", 2.0f)
            .put("index.similarity.my_similarity.b", 0.5f)
            .put("index.similarity.my_similarity.discount_overlaps", false)
            .build();
        MapperService mapperService = createIndex("foo", indexSettings, mapping).mapperService();
        assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(), instanceOf(LegacyBM25Similarity.class));

        LegacyBM25Similarity similarity
            = (LegacyBM25Similarity) mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get();
        assertThat(similarity.getK1(), equalTo(2.0f));
        assertThat(similarity.getB(), equalTo(0.5f));
        assertThat(similarity.getDiscountOverlaps(), equalTo(false));
    }

    public void testResolveSimilaritiesFromMapping_boolean() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "boolean").endObject()
            .endObject()
            .endObject().endObject();

        MapperService mapperService = createIndex("foo", Settings.EMPTY, mapping).mapperService();
        assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(), instanceOf(BooleanSimilarity.class));
    }

    public void testResolveSimilaritiesFromMapping_DFR() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "DFR")
            .put("index.similarity.my_similarity.basic_model", "g")
            .put("index.similarity.my_similarity.after_effect", "l")
            .put("index.similarity.my_similarity.normalization", "h2")
            .put("index.similarity.my_similarity.normalization.h2.c", 3f)
            .build();
        MapperService mapperService = createIndex("foo", indexSettings, mapping).mapperService();
        assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(), instanceOf(DFRSimilarity.class));

        DFRSimilarity similarity = (DFRSimilarity) mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get();
        assertThat(similarity.getBasicModel(), instanceOf(BasicModelG.class));
        assertThat(similarity.getAfterEffect(), instanceOf(AfterEffectL.class));
        assertThat(similarity.getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) similarity.getNormalization()).getC(), equalTo(3f));
    }

    public void testResolveSimilaritiesFromMapping_IB() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "IB")
            .put("index.similarity.my_similarity.distribution", "spl")
            .put("index.similarity.my_similarity.lambda", "ttf")
            .put("index.similarity.my_similarity.normalization", "h2")
            .put("index.similarity.my_similarity.normalization.h2.c", 3f)
            .build();
        MapperService mapperService = createIndex("foo", indexSettings, mapping).mapperService();
        assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(), instanceOf(IBSimilarity.class));

        IBSimilarity similarity = (IBSimilarity) mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get();
        assertThat(similarity.getDistribution(), instanceOf(DistributionSPL.class));
        assertThat(similarity.getLambda(), instanceOf(LambdaTTF.class));
        assertThat(similarity.getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) similarity.getNormalization()).getC(), equalTo(3f));
    }

    public void testResolveSimilaritiesFromMapping_DFI() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "DFI")
            .put("index.similarity.my_similarity.independence_measure", "chisquared")
            .build();
        MapperService mapperService = createIndex("foo", indexSettings, mapping).mapperService();
        MappedFieldType fieldType = mapperService.fieldType("field1");

        assertThat(fieldType.getTextSearchInfo().getSimilarity().get(), instanceOf(DFISimilarity.class));
        DFISimilarity similarity = (DFISimilarity) fieldType.getTextSearchInfo().getSimilarity().get();
        assertThat(similarity.getIndependence(), instanceOf(IndependenceChiSquared.class));
    }

    public void testResolveSimilaritiesFromMapping_LMDirichlet() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "LMDirichlet")
            .put("index.similarity.my_similarity.mu", 3000f)
            .build();

        MapperService mapperService = createIndex("foo", indexSettings, mapping).mapperService();
        assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(), instanceOf(LMDirichletSimilarity.class));

        LMDirichletSimilarity similarity
            = (LMDirichletSimilarity) mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get();
        assertThat(similarity.getMu(), equalTo(3000f));
    }

    public void testResolveSimilaritiesFromMapping_LMJelinekMercer() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "LMJelinekMercer")
            .put("index.similarity.my_similarity.lambda", 0.7f)
            .build();
        MapperService mapperService = createIndex("foo", indexSettings, mapping).mapperService();
        assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(),
            instanceOf(LMJelinekMercerSimilarity.class));

        LMJelinekMercerSimilarity similarity
            = (LMJelinekMercerSimilarity) mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get();
        assertThat(similarity.getLambda(), equalTo(0.7f));
    }

    public void testResolveSimilaritiesFromMapping_Unknown() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "unknown_similarity").endObject()
            .endObject()
            .endObject().endObject());

        IndexService indexService = createIndex("foo");
        try {
            indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            fail("Expected MappingParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Unknown Similarity type [unknown_similarity] for field [field1]"));
        }
    }

    public void testUnknownParameters() throws IOException {
        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "BM25")
            .put("index.similarity.my_similarity.z", 2.0f)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createIndex("foo", indexSettings));
        assertEquals("Unknown settings for similarity of type [BM25]: [z]", e.getMessage());
    }
}
