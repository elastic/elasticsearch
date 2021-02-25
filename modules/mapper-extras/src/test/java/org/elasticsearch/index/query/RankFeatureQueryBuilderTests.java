/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        switch (random().nextInt(4)) {
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
        case 3:
            function = new ScoreFunction.Linear();
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
    protected void doAssertLuceneQuery(RankFeatureQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        Class<?> expectedClass = FeatureField.newSaturationQuery("", "", 1, 1).getClass();
        assertThat(query, either(instanceOf(MatchNoDocsQuery.class)).or(instanceOf(expectedClass)));
    }

    public void testDefaultScoreFunction() throws IOException {
        String query = "{\n" +
                "    \"rank_feature\" : {\n" +
                "        \"field\": \"my_feature_field\"\n" +
                "    }\n" +
                "}";
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertEquals(FeatureField.newSaturationQuery("_feature", "my_feature_field"), parsedQuery);
    }

    public void testIllegalField() {
        String query = "{\n" +
                "    \"rank_feature\" : {\n" +
                "        \"field\": \"" + TEXT_FIELD_NAME + "\"\n" +
                "    }\n" +
                "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parseQuery(query).toQuery(createSearchExecutionContext()));
        assertEquals("[rank_feature] query only works on [rank_feature] fields and features of [rank_features] fields, not [text]",
            e.getMessage());
    }

    public void testIllegalCombination() {
        String query = "{\n" +
                "    \"rank_feature\" : {\n" +
                "        \"field\": \"my_negative_feature_field\",\n" +
                "        \"log\" : {\n" +
                "            \"scaling_factor\": 4.5\n" +
                "        }\n" +
                "    }\n" +
                "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parseQuery(query).toQuery(createSearchExecutionContext()));
        assertEquals(
                "Cannot use the [log] function with a field that has a negative score impact as it would trigger negative scores",
                e.getMessage());
    }
}
