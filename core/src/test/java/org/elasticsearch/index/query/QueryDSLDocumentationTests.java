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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.boostingQuery;
import static org.elasticsearch.index.query.QueryBuilders.commonTermsQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.disMaxQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceRangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoHashCellQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.indicesQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanContainingQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanFirstQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanMultiTermQueryBuilder;
import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanNotQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanOrQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanWithinQuery;
import static org.elasticsearch.index.query.QueryBuilders.templateQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.QueryBuilders.typeQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;

/**
 * If one of the following tests doesn't compile make sure to not only fix the compilation error here
 * but also the documentation under ./docs/java-api/query-dsl/bool-query.asciidoc
 *
 * There are no assertions here on purpose - all of these tests ((ideally) should) equal to what is
 * documented in the java api query dsl part of our reference guide.
 * */
public class QueryDSLDocumentationTests extends ESTestCase {
    public void testBool() {
        boolQuery()
                .must(termQuery("content", "test1"))
                .must(termQuery("content", "test4"))
                .mustNot(termQuery("content", "test2"))
                .should(termQuery("content", "test3"))
                .filter(termQuery("content", "test5"));
    }

    public void testBoosting() {
        boostingQuery(termQuery("name","kimchy"), termQuery("name","dadoonet"))
                .negativeBoost(0.2f);
    }

    public void testCommonTerms() {
        commonTermsQuery("name", "kimchy");
    }

    public void testConstantScore() {
        constantScoreQuery(termQuery("name","kimchy"))
            .boost(2.0f);
    }

    public void testDisMax() {
        disMaxQuery()
                .add(termQuery("name", "kimchy"))
                .add(termQuery("name", "elasticsearch"))
                .boost(1.2f)
                .tieBreaker(0.7f);
    }

    public void testExists() {
        existsQuery("name");
    }

    public void testFunctionScore() {
        FilterFunctionBuilder[] functions = {
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        matchQuery("name", "kimchy"),
                        randomFunction("ABCDEF")),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        exponentialDecayFunction("age", 0L, 1L))
        };
        functionScoreQuery(functions);
    }

    public void testFuzzy() {
        fuzzyQuery("name", "kimchy");
    }

    public void testGeoBoundingBox() {
        geoBoundingBoxQuery("pin.location").setCorners(40.73, -74.1, 40.717, -73.99);
    }

    public void testGeoDistance() {
        geoDistanceQuery("pin.location")
            .point(40, -70)
            .distance(200, DistanceUnit.KILOMETERS)
            .optimizeBbox("memory")                     // TODO switch to geoexectype see also bounding box
            .geoDistance(GeoDistance.ARC);
    }

    public void testGeoDistanceRange() {
        geoDistanceRangeQuery("pin.location", new GeoPoint(40, -70)) // TODO check why I need the point here but not above
            .from("200km")
            .to("400km")
            .includeLower(true)
            .includeUpper(false)
            .optimizeBbox("memory")
            .geoDistance(GeoDistance.ARC);
    }

    public void testGeoPolygon() {
        List<GeoPoint> points = new ArrayList<GeoPoint>();
        points.add(new GeoPoint(40, -70));
        points.add(new GeoPoint(30, -80));
        points.add(new GeoPoint(20, -90));
        geoPolygonQuery("pin.location", points);
    }

    public void testGeoShape() throws IOException {
        GeoShapeQueryBuilder qb = geoShapeQuery(
                "pin.location",
                ShapeBuilders.newMultiPoint(
                        new CoordinatesBuilder()
                    .coordinate(0, 0)
                    .coordinate(0, 10)
                    .coordinate(10, 10)
                    .coordinate(10, 0)
                    .coordinate(0, 0)
                    .build()));
        qb.relation(ShapeRelation.WITHIN);

        qb = geoShapeQuery(
                    "pin.location",
                    "DEU",
                    "countries");
        qb.relation(ShapeRelation.WITHIN)
            .indexedShapeIndex("shapes")
            .indexedShapePath("location");
    }

    public void testGeoHashCell() {
        geoHashCellQuery("pin.location",
                new GeoPoint(13.4080, 52.5186))
            .neighbors(true)
            .precision(3);
    }

    public void testHasChild() {
        hasChildQuery(
                "blog_tag",
                termQuery("tag","something"),
                ScoreMode.None);
    }

    public void testHasParent() {
        hasParentQuery(
            "blog",
            termQuery("tag","something"),
                false);
    }

    public void testIds() {
        idsQuery("my_type", "type2")
                .addIds("1", "4", "100");

        idsQuery().addIds("1", "4", "100");
    }

    public void testIndices() {
        indicesQuery(
                termQuery("tag", "wow"),
                "index1", "index2"
            ).noMatchQuery(termQuery("tag", "kow"));

        indicesQuery(
                termQuery("tag", "wow"),
                "index1", "index2"
            ).noMatchQuery("all");
    }

    public void testMatchAll() {
        matchAllQuery();
    }

    public void testMatch() {
        matchQuery("name", "kimchy elasticsearch");
    }

    public void testMLT() {
        String[] fields = {"name.first", "name.last"};
        String[] texts = {"text like this one"};
        Item[] items = null;

        moreLikeThisQuery(fields, texts, items)
        .minTermFreq(1)
        .maxQueryTerms(12);
    }

    public void testMultiMatch() {
        multiMatchQuery("kimchy elasticsearch", "user", "message");
    }

    public void testNested() {
        nestedQuery(
                "obj1",
                boolQuery()
                        .must(matchQuery("obj1.name", "blue"))
                        .must(rangeQuery("obj1.count").gt(5)),
                ScoreMode.Avg);
    }

    public void testPrefix() {
        prefixQuery("brand", "heine");
    }

    public void testQueryString() {
        queryStringQuery("+kimchy -elasticsearch");
    }

    public void testRange() {
        rangeQuery("price")
        .from(5)
        .to(10)
        .includeLower(true)
        .includeUpper(false);

        rangeQuery("age")
        .gte("10")
        .lt("20");
    }

    public void testRegExp() {
        regexpQuery("name.first", "s.*y");
    }

    public void testScript() {
        scriptQuery(
                new Script("doc['num1'].value > 1")
            );

        Map<String, Integer> parameters = new HashMap<>();
        parameters.put("param1", 5);
        scriptQuery(
                new Script(
                    "mygroovyscript",
                    ScriptType.FILE,
                    "groovy",
                    parameters)
            );

    }

    public void testSimpleQueryString() {
        simpleQueryStringQuery("+kimchy -elasticsearch");
    }

    public void testSpanContaining() {
        spanContainingQuery(
                spanNearQuery(spanTermQuery("field1","bar"), 5)
                    .addClause(spanTermQuery("field1","baz"))
                    .inOrder(true),
                spanTermQuery("field1","foo"));
    }

    public void testSpanFirst() {
        spanFirstQuery(
                spanTermQuery("user", "kimchy"),
                3
            );
    }

    public void testSpanMultiTerm() {
        spanMultiTermQueryBuilder(prefixQuery("user", "ki"));
    }

    public void testSpanNear() {
        spanNearQuery(spanTermQuery("field","value1"), 12)
        .addClause(spanTermQuery("field","value2"))
        .addClause(spanTermQuery("field","value3"))
        .inOrder(false);
    }

    public void testSpanNot() {
        spanNotQuery(spanTermQuery("field","value1"),
                spanTermQuery("field","value2"));
    }

    public void testSpanOr() {
        spanOrQuery(spanTermQuery("field","value1"))
        .addClause(spanTermQuery("field","value2"))
        .addClause(spanTermQuery("field","value3"));
    }

    public void testSpanTerm() {
        spanTermQuery("user", "kimchy");
    }

    public void testSpanWithin() {
        spanWithinQuery(
                spanNearQuery(spanTermQuery("field1", "bar"), 5)
                    .addClause(spanTermQuery("field1", "baz"))
                    .inOrder(true),
                spanTermQuery("field1", "foo"));
    }

    public void testTemplate() {
        templateQuery(
                "gender_template",
                ScriptType.STORED,
                new HashMap<>());
    }

    public void testTerm() {
        termQuery("name", "kimchy");
    }

    public void testTerms() {
        termsQuery("tags", "blue", "pill");
    }

    public void testType() {
        typeQuery("my_type");
    }

    public void testWildcard() {
        wildcardQuery("user", "k?mch*");
    }
}
