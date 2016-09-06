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

import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.IndependenceSaturated;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;

public class SimilarityTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testResolveDefaultSimilarities() {
        SimilarityService similarityService = createIndex("foo").similarityService();
        assertThat(similarityService.getSimilarity("classic").get(), instanceOf(ClassicSimilarity.class));
        assertThat(similarityService.getSimilarity("BM25").get(), instanceOf(BM25Similarity.class));
        assertThat(similarityService.getSimilarity("default"), equalTo(null));
    }

    public void testResolveSimilaritiesFromMapping_classic() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "classic")
            .put("index.similarity.my_similarity.discount_overlaps", false)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(ClassicSimilarityProvider.class));

        ClassicSimilarityProvider provider =
            (ClassicSimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
        assertThat(provider.get().getDiscountOverlaps(), equalTo(false));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "BM25")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", true)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }
    }

    public void testResolveSimilaritiesFromMapping_bm25() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "BM25")
            .put("index.similarity.my_similarity.k1", 2.0f)
            .put("index.similarity.my_similarity.b", 0.5f)
            .put("index.similarity.my_similarity.discount_overlaps", false)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(BM25SimilarityProvider.class));

        BM25SimilarityProvider provider =
            (BM25SimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
        assertThat(provider.get().getK1(), equalTo(2.0f));
        assertThat(provider.get().getB(), equalTo(0.5f));
        assertThat(provider.get().getDiscountOverlaps(), equalTo(false));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "classic")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", true)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.b", 0.7f)
                .put("index.similarity.my_similarity.k1", 2.7f)
                .build();
            client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet();
            assertThat(provider.get().getB(), equalTo(0.7f));
            assertThat(provider.get().getK1(), equalTo(2.7f));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.b", 2.0f)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("illegal b value: 2.0, must be between 0 and 1"));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.k1", -1.0f)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("illegal k1 value: -1.0, must be a non-negative finite value"));
        }
    }

    public void testResolveSimilaritiesFromMapping_DFR() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "DFR")
            .put("index.similarity.my_similarity.basic_model", "g")
            .put("index.similarity.my_similarity.after_effect", "l")
            .put("index.similarity.my_similarity.normalization", "h2")
            .put("index.similarity.my_similarity.normalization.h2.c", 3f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(DFRSimilarityProvider.class));

        DFRSimilarityProvider provider =
            (DFRSimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
        assertThat(provider.get().getBasicModel(), instanceOf(BasicModelG.class));
        assertThat(provider.get().getAfterEffect(), instanceOf(AfterEffectL.class));
        assertThat(provider.get().getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) provider.get().getNormalization()).getC(), equalTo(3f));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "classic")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.basic_model", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Unsupported BasicModel [foo]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.after_effect", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Unsupported AfterEffect [foo]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.basic_model", "in")
                .put("index.similarity.my_similarity.after_effect", "b")
                .build();
            client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet();
            assertThat(provider.get().getBasicModel(), instanceOf(BasicModelIn.class));
            assertThat(provider.get().getAfterEffect(), instanceOf(AfterEffectB.class));
        }
    }

    public void testResolveSimilaritiesFromMapping_IB() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "IB")
            .put("index.similarity.my_similarity.distribution", "spl")
            .put("index.similarity.my_similarity.collection_model", "ttf")
            .put("index.similarity.my_similarity.normalization", "h2")
            .put("index.similarity.my_similarity.normalization.h2.c", 3f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(IBSimilarityProvider.class));

        IBSimilarityProvider provider =
            (IBSimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
        assertThat(provider.get().getDistribution(), instanceOf(DistributionSPL.class));
        assertThat(provider.get().getLambda(), instanceOf(LambdaTTF.class));
        assertThat(provider.get().getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) provider.get().getNormalization()).getC(), equalTo(3f));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "classic")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.distribution", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Unsupported Distribution [foo]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.collection_model", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Unsupported CollectionModel [foo]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.distribution", "ll")
                .put("index.similarity.my_similarity.collection_model", "df")
                .build();
            client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet();
            assertThat(provider.get().getDistribution(), instanceOf(DistributionLL.class));
            assertThat(provider.get().getLambda(), instanceOf(LambdaDF.class));
        }
    }

    public void testResolveSimilaritiesFromMapping_DFI() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "DFI")
            .put("index.similarity.my_similarity.independence_measure", "chisquared")
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        MappedFieldType fieldType = documentMapper.mappers().getMapper("field1").fieldType();
        assertThat(fieldType.similarity(), instanceOf(DFISimilarityProvider.class));
        DFISimilarityProvider provider = (DFISimilarityProvider) fieldType.similarity();
        assertThat(provider.get().getIndependence(), instanceOf(IndependenceChiSquared.class));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "classic")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.independence_measure", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Unsupported IndependenceMeasure [foo]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.independence_measure", "saturated")
                .build();
            client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet();
            assertThat(provider.get().getIndependence(), instanceOf(IndependenceSaturated.class));
        }
    }

    public void testResolveSimilaritiesFromMapping_LMDirichlet() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "LMDirichlet")
            .put("index.similarity.my_similarity.mu", 3000f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(LMDirichletSimilarityProvider.class));

        LMDirichletSimilarityProvider provider =
            (LMDirichletSimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
        assertThat(provider.get().getMu(), equalTo(3000f));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "classic")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.mu", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Failed to parse value [foo] for setting [index.similarity.my_similarity.mu]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.mu", 2000f)
                .build();
            client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet();
            assertThat(provider.get().getMu(), equalTo(2000f));
        }
    }

    public void testResolveSimilaritiesFromMapping_LMJelinekMercer() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "LMJelinekMercer")
            .put("index.similarity.my_similarity.lambda", 0.7f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(LMJelinekMercerSimilarityProvider.class));

        LMJelinekMercerSimilarityProvider provider =
            (LMJelinekMercerSimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
        assertThat(provider.get().getLambda(), equalTo(0.7f));

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.type", "classic")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.type]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.discount_overlaps", false)
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(),
                startsWith("Can't update non dynamic settings [[index.similarity.my_similarity.discount_overlaps]]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.lambda", "foo")
                .build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet());
            assertThat(e.getMessage(), startsWith("Failed to parse value [foo] for setting [index.similarity.my_similarity.lambda]"));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(true));
        }

        {
            Settings newSettings = Settings.builder()
                .put("index.similarity.my_similarity.lambda", 0.9f)
                .build();
            client().admin().indices().prepareUpdateSettings("foo").setSettings(newSettings).execute().actionGet();
            assertThat(provider.get().getLambda(), equalTo(0.9f));
        }
    }

    public void testResolveSimilaritiesFromMapping_Unknown() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "unknown_similarity").endObject()
            .endObject()
            .endObject().endObject().string();

        IndexService indexService = createIndex("foo");
        try {
            indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            fail("Expected MappingParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Unknown Similarity type [unknown_similarity] for field [field1]"));
        }
    }

    public void testSimilarityDefaultBackCompat() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1")
            .field("similarity", "default")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject().string();
        Settings settings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.V_2_2_0))
            .build();

        DocumentMapperParser parser = createIndex("test_v2.x", settings).mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(), instanceOf(ClassicSimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity().name(), equalTo("classic"));

        parser = createIndex("test_v3.x").mapperService().documentMapperParser();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("Expected MappingParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Unknown Similarity type [default] for field [field1]"));
        }
    }

    public void testMultipleSimilarities() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity1").endObject()
            .startObject("field2").field("type", "text").field("similarity", "my_similarity2").endObject()
            .startObject("field3").field("type", "text").field("similarity", "my_similarity3").endObject()
            .startObject("field4").field("type", "text").field("similarity", "my_similarity4").endObject()
            .startObject("field5").field("type", "text").field("similarity", "my_similarity5").endObject()
            .startObject("field6").field("type", "text").field("similarity", "my_similarity6").endObject()
            .startObject("field7").field("type", "text").field("similarity", "my_similarity7").endObject()
            .endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity1.type", "classic")
            .put("index.similarity.my_similarity1.discount_overlaps", false)
            .put("index.similarity.my_similarity2.type", "BM25")
            .put("index.similarity.my_similarity2.k1", 2.0f)
            .put("index.similarity.my_similarity2.b", 0.5f)
            .put("index.similarity.my_similarity2.discount_overlaps", false)
            .put("index.similarity.my_similarity3.type", "DFR")
            .put("index.similarity.my_similarity3.basic_model", "g")
            .put("index.similarity.my_similarity3.after_effect", "l")
            .put("index.similarity.my_similarity3.normalization", "h2")
            .put("index.similarity.my_similarity3.normalization.h2.c", 3f)
            .put("index.similarity.my_similarity4.type", "IB")
            .put("index.similarity.my_similarity4.distribution", "spl")
            .put("index.similarity.my_similarity4.collection_model", "ttf")
            .put("index.similarity.my_similarity4.normalization", "h2")
            .put("index.similarity.my_similarity4.normalization.h2.c", 3f)
            .put("index.similarity.my_similarity5.type", "DFI")
            .put("index.similarity.my_similarity5.independence_measure", "chisquared")
            .put("index.similarity.my_similarity6.type", "LMDirichlet")
            .put("index.similarity.my_similarity6.mu", 3000f)
            .put("index.similarity.my_similarity7.type", "LMJelinekMercer")
            .put("index.similarity.my_similarity7.lambda", 0.7f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getMapper("field1").fieldType().similarity(),
            instanceOf(ClassicSimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field2").fieldType().similarity(),
            instanceOf(BM25SimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field3").fieldType().similarity(),
            instanceOf(DFRSimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field4").fieldType().similarity(),
            instanceOf(IBSimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field5").fieldType().similarity(),
            instanceOf(DFISimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field6").fieldType().similarity(),
            instanceOf(LMDirichletSimilarityProvider.class));
        assertThat(documentMapper.mappers().getMapper("field7").fieldType().similarity(),
            instanceOf(LMJelinekMercerSimilarityProvider.class));

        {
            ClassicSimilarityProvider provider =
                (ClassicSimilarityProvider) documentMapper.mappers().getMapper("field1").fieldType().similarity();
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }

        {
            BM25SimilarityProvider provider =
                (BM25SimilarityProvider) documentMapper.mappers().getMapper("field2").fieldType().similarity();
            assertThat(provider.get().getK1(), equalTo(2.0f));
            assertThat(provider.get().getB(), equalTo(0.5f));
            assertThat(provider.get().getDiscountOverlaps(), equalTo(false));
        }

        {
            DFRSimilarityProvider provider =
                (DFRSimilarityProvider) documentMapper.mappers().getMapper("field3").fieldType().similarity();
            assertThat(provider.get().getBasicModel(), instanceOf(BasicModelG.class));
            assertThat(provider.get().getAfterEffect(), instanceOf(AfterEffectL.class));
            assertThat(provider.get().getNormalization(), instanceOf(NormalizationH2.class));
            assertThat(((NormalizationH2) provider.get().getNormalization()).getC(), equalTo(3f));
        }

        {
            IBSimilarityProvider provider =
                (IBSimilarityProvider) documentMapper.mappers().getMapper("field4").fieldType().similarity();
            assertThat(provider.get().getDistribution(), instanceOf(DistributionSPL.class));
            assertThat(provider.get().getLambda(), instanceOf(LambdaTTF.class));
            assertThat(provider.get().getNormalization(), instanceOf(NormalizationH2.class));
            assertThat(((NormalizationH2) provider.get().getNormalization()).getC(), equalTo(3f));
        }

        {
            DFISimilarityProvider provider =
                (DFISimilarityProvider) documentMapper.mappers().getMapper("field5").fieldType().similarity();
            assertThat(provider.get().getIndependence(), instanceOf(IndependenceChiSquared.class));
        }

        {
            LMDirichletSimilarityProvider provider =
                (LMDirichletSimilarityProvider) documentMapper.mappers().getMapper("field6").fieldType().similarity();
            assertThat(provider.get().getMu(), equalTo(3000f));
        }

        {
            LMJelinekMercerSimilarityProvider provider =
                (LMJelinekMercerSimilarityProvider) documentMapper.mappers().getMapper("field7").fieldType().similarity();
            assertThat(provider.get().getLambda(), equalTo(0.7f));
        }
    }
}
