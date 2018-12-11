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

import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class LegacySimilarityTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testResolveDefaultSimilaritiesOn6xIndex() {
        final Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0) // otherwise classic is forbidden
                .build();
        final SimilarityService similarityService = createIndex("foo", indexSettings).similarityService();
        assertThat(similarityService.getSimilarity("classic").get(), instanceOf(ClassicSimilarity.class));
        assertWarnings("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                + "instead.");
        assertThat(similarityService.getSimilarity("BM25").get(), instanceOf(LegacyBM25Similarity.class));
        assertThat(similarityService.getSimilarity("boolean").get(), instanceOf(BooleanSimilarity.class));
        assertThat(similarityService.getSimilarity("default"), equalTo(null));
    }

    public void testResolveSimilaritiesFromMappingClassic() throws IOException {
        try (XContentBuilder mapping = XContentFactory.jsonBuilder()) {
            mapping.startObject();
            {
                mapping.startObject("type");
                {
                    mapping.startObject("properties");
                    {
                        mapping.startObject("field1");
                        {
                            mapping.field("type", "text");
                            mapping.field("similarity", "my_similarity");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();

            final Settings indexSettings = Settings.builder()
                    .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_6_3_0) // otherwise classic is forbidden
                    .put("index.similarity.my_similarity.type", "classic")
                    .put("index.similarity.my_similarity.discount_overlaps", false)
                    .build();
            final MapperService mapperService = createIndex("foo", indexSettings, "type", mapping).mapperService();
            assertThat(mapperService.fullName("field1").similarity().get(), instanceOf(ClassicSimilarity.class));

            final ClassicSimilarity similarity = (ClassicSimilarity) mapperService.fullName("field1").similarity().get();
            assertThat(similarity.getDiscountOverlaps(), equalTo(false));
        }
    }

}
