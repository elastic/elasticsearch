/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.Score;
import org.elasticsearch.xpack.sql.querydsl.agg.AvgAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByColumnKey;
import org.elasticsearch.xpack.sql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScoreSort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Missing;
import org.elasticsearch.xpack.sql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.KeywordEsField;

import static java.util.Collections.singletonList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.search.sort.SortBuilders.scoreSort;

public class SourceGeneratorTests extends ESTestCase {

    public void testNoQueryNoFilter() {
        QueryContainer container = new QueryContainer();
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertNull(sourceBuilder.query());
    }

    public void testQueryNoFilter() {
        QueryContainer container = new QueryContainer().with(new MatchQuery(Location.EMPTY, "foo", "bar"));
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(matchQuery("foo", "bar").operator(Operator.OR), sourceBuilder.query());
    }

    public void testNoQueryFilter() {
        QueryContainer container = new QueryContainer();
        QueryBuilder filter = matchQuery("bar", "baz");
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, randomIntBetween(1, 10));
        assertEquals(boolQuery().filter(matchQuery("bar", "baz")), sourceBuilder.query());
    }

    public void testQueryFilter() {
        QueryContainer container = new QueryContainer().with(new MatchQuery(Location.EMPTY, "foo", "bar"));
        QueryBuilder filter = matchQuery("bar", "baz");
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, filter, randomIntBetween(1, 10));
        assertEquals(boolQuery().must(matchQuery("foo", "bar").operator(Operator.OR)).filter(matchQuery("bar", "baz")),
                sourceBuilder.query());
    }

    public void testLimit() {
        QueryContainer container = new QueryContainer().withLimit(10).addGroups(singletonList(new GroupByColumnKey("1", "field")));
        int size = randomIntBetween(1, 10);
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, size);
        Builder aggBuilder = sourceBuilder.aggregations();
        assertEquals(1, aggBuilder.count());
        CompositeAggregationBuilder composite = (CompositeAggregationBuilder) aggBuilder.getAggregatorFactories().iterator().next();
        assertEquals(size, composite.size());
    }

    public void testSortNoneSpecified() {
        QueryContainer container = new QueryContainer();
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(singletonList(fieldSort("_doc")), sourceBuilder.sorts());
    }

    public void testSelectScoreForcesTrackingScore() {
        QueryContainer container = new QueryContainer()
                .addColumn(new Score(new Location(1, 1)).toAttribute());
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertTrue(sourceBuilder.trackScores());
    }

    public void testSortScoreSpecified() {
        QueryContainer container = new QueryContainer()
                .sort(new ScoreSort(Direction.DESC, null));
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(singletonList(scoreSort()), sourceBuilder.sorts());
    }

    public void testSortFieldSpecified() {
        FieldSortBuilder sortField = fieldSort("test").unmappedType("keyword");
        
        QueryContainer container = new QueryContainer()
                .sort(new AttributeSort(new FieldAttribute(new Location(1, 1), "test", new KeywordEsField("test")), Direction.ASC,
                        Missing.LAST));
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(singletonList(sortField.order(SortOrder.ASC).missing("_last")), sourceBuilder.sorts());

        container = new QueryContainer()
                .sort(new AttributeSort(new FieldAttribute(new Location(1, 1), "test", new KeywordEsField("test")), Direction.DESC,
                        Missing.FIRST));
        sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertEquals(singletonList(sortField.order(SortOrder.DESC).missing("_first")), sourceBuilder.sorts());
    }

    public void testNoSort() {
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(new QueryContainer(), null, randomIntBetween(1, 10));
        assertEquals(singletonList(fieldSort("_doc").order(SortOrder.ASC)), sourceBuilder.sorts());
    }

    public void testNoSortIfAgg() {
        QueryContainer container = new QueryContainer()
                .addGroups(singletonList(new GroupByColumnKey("group_id", "group_column")))
                .addAgg("group_id", new AvgAgg("agg_id", "avg_column"));
        SearchSourceBuilder sourceBuilder = SourceGenerator.sourceBuilder(container, null, randomIntBetween(1, 10));
        assertNull(sourceBuilder.sorts());
    }
}