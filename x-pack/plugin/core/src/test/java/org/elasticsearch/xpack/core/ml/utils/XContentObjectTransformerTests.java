/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class XContentObjectTransformerTests extends ESTestCase {

    public void testFromMap() throws IOException {
        Map<String, Object> aggMap = Collections.singletonMap("fieldName",
            Collections.singletonMap("max",
                Collections.singletonMap("field", "fieldName")));

        XContentObjectTransformer<AggregatorFactories.Builder> aggTransformer = XContentObjectTransformer.aggregatorTransformer();
        assertXContentAreEqual(aggTransformer.fromMap(aggMap), aggMap);
        assertXContentAreEqual(aggTransformer.fromMap(aggMap), aggTransformer.toMap(aggTransformer.fromMap(aggMap)));

        Map<String, Object> queryMap = Collections.singletonMap("match",
            Collections.singletonMap("fieldName", new HashMap<String, Object>(){{
                // Add all the default fields so they are not added dynamically when the object is parsed
                put("query","fieldValue");
                put("operator","OR");
                put("prefix_length",0);
                put("max_expansions",50);
                put("fuzzy_transpositions",true);
                put("lenient",false);
                put("zero_terms_query","NONE");
                put("auto_generate_synonyms_phrase_query",true);
                put("boost",1.0);
            }}));

        XContentObjectTransformer<QueryBuilder> queryBuilderTransformer = XContentObjectTransformer.queryBuilderTransformer();
        assertXContentAreEqual(queryBuilderTransformer.fromMap(queryMap), queryMap);
        assertXContentAreEqual(queryBuilderTransformer.fromMap(queryMap),
            queryBuilderTransformer.toMap(queryBuilderTransformer.fromMap(queryMap)));
    }

    public void testToMap() throws IOException {
        XContentObjectTransformer<AggregatorFactories.Builder> aggTransformer = XContentObjectTransformer.aggregatorTransformer();
        XContentObjectTransformer<QueryBuilder> queryBuilderTransformer = XContentObjectTransformer.queryBuilderTransformer();

        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        long aggHistogramInterval = randomNonNegativeLong();
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        aggs.addAggregator(AggregationBuilders.dateHistogram("buckets")
            .interval(aggHistogramInterval).subAggregation(maxTime).field("time"));

        assertXContentAreEqual(aggs, aggTransformer.toMap(aggs));
        assertXContentAreEqual(aggTransformer.fromMap(aggTransformer.toMap(aggs)), aggTransformer.toMap(aggs));

        QueryBuilder queryBuilder = QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10));

        assertXContentAreEqual(queryBuilder, queryBuilderTransformer.toMap(queryBuilder));
        assertXContentAreEqual(queryBuilderTransformer.fromMap(queryBuilderTransformer.toMap(queryBuilder)),
            queryBuilderTransformer.toMap(queryBuilder));
    }

    private void assertXContentAreEqual(ToXContentObject object, Map<String, Object> map) throws IOException {
        XContentType xContentType = XContentType.JSON;
        BytesReference objectReference = XContentHelper.toXContent(object, xContentType, EMPTY_PARAMS, false);
        BytesReference mapReference = BytesReference.bytes(XContentFactory.jsonBuilder().map(map));
        assertToXContentEquivalent(objectReference, mapReference, xContentType);
    }
}
