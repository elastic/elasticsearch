/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.search.similarities.*;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimilarityTests extends ElasticsearchTestCase {

    @Test
    public void testResolveDefaultSimilarities() {
        SimilarityLookupService similarityLookupService = similarityService().similarityLookupService();
        assertThat(similarityLookupService.similarity("default"), instanceOf(PreBuiltSimilarityProvider.class));
        assertThat(similarityLookupService.similarity("default").get(), instanceOf(DefaultSimilarity.class));
        assertThat(similarityLookupService.similarity("BM25"), instanceOf(PreBuiltSimilarityProvider.class));
        assertThat(similarityLookupService.similarity("BM25").get(), instanceOf(BM25Similarity.class));
    }

    @Test
    public void testResolveSimilaritiesFromMapping_default() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("similarity", "my_similarity").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "default")
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
        SimilarityService similarityService = similarityService(indexSettings);
        DocumentMapper documentMapper = similarityService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().similarity(), instanceOf(DefaultSimilarityProvider.class));

        DefaultSimilarity similarity = (DefaultSimilarity) documentMapper.mappers().name("field1").mapper().similarity().get();
        assertThat(similarity.getDiscountOverlaps(), equalTo(false));
    }

    @Test
    public void testResolveSimilaritiesFromMapping_bm25() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("similarity", "my_similarity").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "BM25")
                .put("index.similarity.my_similarity.k1", 2.0f)
                .put("index.similarity.my_similarity.b", 1.5f)
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
        SimilarityService similarityService = similarityService(indexSettings);
        DocumentMapper documentMapper = similarityService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().similarity(), instanceOf(BM25SimilarityProvider.class));

        BM25Similarity similarity = (BM25Similarity) documentMapper.mappers().name("field1").mapper().similarity().get();
        assertThat(similarity.getK1(), equalTo(2.0f));
        assertThat(similarity.getB(), equalTo(1.5f));
        assertThat(similarity.getDiscountOverlaps(), equalTo(false));
    }

    @Test
    public void testResolveSimilaritiesFromMapping_DFR() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("similarity", "my_similarity").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "DFR")
                .put("index.similarity.my_similarity.basic_model", "g")
                .put("index.similarity.my_similarity.after_effect", "l")
                .put("index.similarity.my_similarity.normalization", "h2")
                .put("index.similarity.my_similarity.normalization.h2.c", 3f)
                .build();
        SimilarityService similarityService = similarityService(indexSettings);
        DocumentMapper documentMapper = similarityService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().similarity(), instanceOf(DFRSimilarityProvider.class));

        DFRSimilarity similarity = (DFRSimilarity) documentMapper.mappers().name("field1").mapper().similarity().get();
        assertThat(similarity.getBasicModel(), instanceOf(BasicModelG.class));
        assertThat(similarity.getAfterEffect(), instanceOf(AfterEffectL.class));
        assertThat(similarity.getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) similarity.getNormalization()).getC(), equalTo(3f));
    }

    @Test
    public void testResolveSimilaritiesFromMapping_IB() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("similarity", "my_similarity").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.similarity.my_similarity.type", "IB")
                .put("index.similarity.my_similarity.distribution", "spl")
                .put("index.similarity.my_similarity.lambda", "ttf")
                .put("index.similarity.my_similarity.normalization", "h2")
                .put("index.similarity.my_similarity.normalization.h2.c", 3f)
                .build();
        SimilarityService similarityService = similarityService(indexSettings);
        DocumentMapper documentMapper = similarityService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().similarity(), instanceOf(IBSimilarityProvider.class));

        IBSimilarity similarity = (IBSimilarity) documentMapper.mappers().name("field1").mapper().similarity().get();
        assertThat(similarity.getDistribution(), instanceOf(DistributionSPL.class));
        assertThat(similarity.getLambda(), instanceOf(LambdaTTF.class));
        assertThat(similarity.getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) similarity.getNormalization()).getC(), equalTo(3f));
    }

    private static SimilarityService similarityService() {
        return similarityService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    private static SimilarityService similarityService(Settings settings) {
        Index index = new Index("test");
        Injector injector = new ModulesBuilder()
                .add(new SettingsModule(settings))
                .add(new IndexNameModule(index))
                .add(new IndexSettingsModule(index, settings))
                .add(new CodecModule(settings))
                .add(new MapperServiceModule())
                .add(new AnalysisModule(settings))
                .add(new SimilarityModule(settings))
                .createInjector();
        return injector.getInstance(SimilarityService.class);
    }
}
