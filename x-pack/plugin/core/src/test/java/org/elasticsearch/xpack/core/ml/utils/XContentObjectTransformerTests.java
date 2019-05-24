/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class XContentObjectTransformerTests extends ESTestCase {

    @Override
    public NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testFromMap() throws IOException {
        Map<String, Object> aggMap = Collections.singletonMap("fieldName",
            Collections.singletonMap("max",
                Collections.singletonMap("field", "fieldName")));

        XContentObjectTransformer<AggregatorFactories.Builder> aggTransformer =
            XContentObjectTransformer.aggregatorTransformer(xContentRegistry());
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

        XContentObjectTransformer<QueryBuilder> queryBuilderTransformer =
            XContentObjectTransformer.queryBuilderTransformer(xContentRegistry());
        assertXContentAreEqual(queryBuilderTransformer.fromMap(queryMap), queryMap);
        assertXContentAreEqual(queryBuilderTransformer.fromMap(queryMap),
            queryBuilderTransformer.toMap(queryBuilderTransformer.fromMap(queryMap)));
    }

    public void testFromMapWithBadMaps() {
        Map<String, Object> queryMap = Collections.singletonMap("match",
            Collections.singletonMap("airline", new HashMap<String, Object>() {{
                put("query", "notSupported");
                put("type", "phrase"); //phrase stopped being supported for match in 6.x
            }}));

        XContentObjectTransformer<QueryBuilder> queryBuilderTransformer =
            XContentObjectTransformer.queryBuilderTransformer(xContentRegistry());
        ParsingException exception = expectThrows(ParsingException.class,
            () -> queryBuilderTransformer.fromMap(queryMap));

        assertThat(exception.getMessage(), equalTo("[match] query does not support [type]"));

        Map<String, Object> aggMap = Collections.singletonMap("badTerms",
            Collections.singletonMap("terms", new HashMap<String, Object>() {{
                put("size", 0); //size being 0 in terms agg stopped being supported in 6.x
                put("field", "myField");
            }}));

        XContentObjectTransformer<AggregatorFactories.Builder> aggTransformer =
            XContentObjectTransformer.aggregatorTransformer(xContentRegistry());
        XContentParseException xContentParseException = expectThrows(XContentParseException.class, () -> aggTransformer.fromMap(aggMap));
        assertThat(xContentParseException.getMessage(), containsString("[terms] failed to parse field [size]"));
    }

    public void testToMap() throws IOException {
        XContentObjectTransformer<AggregatorFactories.Builder> aggTransformer =
            XContentObjectTransformer.aggregatorTransformer(xContentRegistry());
        XContentObjectTransformer<QueryBuilder> queryBuilderTransformer =
            XContentObjectTransformer.queryBuilderTransformer(xContentRegistry());

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

    public void testDeprecationWarnings() throws IOException {
        XContentObjectTransformer<QueryBuilder> queryBuilderTransformer = new XContentObjectTransformer<>(NamedXContentRegistry.EMPTY,
            (p)-> {
            p.getDeprecationHandler().usedDeprecatedField("oldField", "newField");
            p.getDeprecationHandler().usedDeprecatedName("oldName", "modernName");
            return new BoolQueryBuilder();
            });
        List<String> deprecations = new ArrayList<>();
        queryBuilderTransformer.fromMap(Collections.singletonMap("bool", "match"), deprecations);

        assertThat(deprecations, hasSize(2));
        assertThat(deprecations, hasItem("Deprecated field [oldField] used, replaced by [newField]"));
        assertThat(deprecations, hasItem("Deprecated field [oldName] used, expected [modernName] instead"));
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    private void assertXContentAreEqual(ToXContentObject object, Map<String, Object> map) throws IOException {
        XContentType xContentType = XContentType.JSON;
        BytesReference objectReference = XContentHelper.toXContent(object, xContentType, EMPTY_PARAMS, false);
        BytesReference mapReference = BytesReference.bytes(XContentFactory.jsonBuilder().map(map));
        assertToXContentEquivalent(objectReference, mapReference, xContentType);
    }
}
