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

package org.elasticsearch.index.query;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperExtrasPlugin;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.RankFeatureQueryBuilder.ScoreFunction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.either;

public class RankFeatureQueryBuilderTests extends AbstractQueryTestCase<RankFeatureQueryBuilder> {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
            "my_feature_field", "type=rank_feature",
            "my_negative_feature_field", "type=rank_feature,positive_score_impact=false",
            "my_feature_vector_field", "type=rank_features"))), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(MapperExtrasPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected RankFeatureQueryBuilder doCreateTestQueryBuilder() {
        ScoreFunction function;
        boolean mayUseNegativeField = true;
        switch (random().nextInt(3)) {
        case 0:
            mayUseNegativeField = false;
            function = new ScoreFunction.Log(1 + randomFloat());
            break;
        case 1:
            if (randomBoolean()) {
                function = new ScoreFunction.Saturation();
            } else {
                function = new ScoreFunction.Saturation(randomFloat());
            }
            break;
        case 2:
            function = new ScoreFunction.Sigmoid(randomFloat(), randomFloat());
            break;
        default:
            throw new AssertionError();
        }
        List<String> fields = new ArrayList<>();
        fields.add("my_feature_field");
        fields.add("unmapped_field");
        fields.add("my_feature_vector_field.feature");
        if (mayUseNegativeField) {
            fields.add("my_negative_feature_field");
        }

        final String field = randomFrom(fields);
        return new RankFeatureQueryBuilder(field, function);
    }

    @Override
    protected void doAssertLuceneQuery(RankFeatureQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Class<?> expectedClass = FeatureField.newSaturationQuery("", "", 1, 1).getClass();
        assertThat(query, either(instanceOf(MatchNoDocsQuery.class)).or(instanceOf(expectedClass)));
    }

    public void testDefaultScoreFunction() throws IOException {
        String query = "{\n" +
                "    \"rank_feature\" : {\n" +
                "        \"field\": \"my_feature_field\"\n" +
                "    }\n" +
                "}";
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertEquals(FeatureField.newSaturationQuery("_feature", "my_feature_field"), parsedQuery);
    }

    public void testIllegalField() throws IOException {
        String query = "{\n" +
                "    \"rank_feature\" : {\n" +
                "        \"field\": \"" + TEXT_FIELD_NAME + "\"\n" +
                "    }\n" +
                "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(query).toQuery(createShardContext()));
        assertEquals("[rank_feature] query only works on [rank_feature] fields and features of [rank_features] fields, not [text]",
            e.getMessage());
    }

    public void testIllegalCombination() throws IOException {
        String query = "{\n" +
                "    \"rank_feature\" : {\n" +
                "        \"field\": \"my_negative_feature_field\",\n" +
                "        \"log\" : {\n" +
                "            \"scaling_factor\": 4.5\n" +
                "        }\n" +
                "    }\n" +
                "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(query).toQuery(createShardContext()));
        assertEquals(
                "Cannot use the [log] function with a field that has a negative score impact as it would trigger negative scores",
                e.getMessage());
    }
}
