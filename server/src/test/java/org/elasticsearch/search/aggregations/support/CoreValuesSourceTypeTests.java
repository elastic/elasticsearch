/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CoreValuesSourceTypeTests extends MapperServiceTestCase {

    public void testFromString() {
        assertThat(CoreValuesSourceType.fromString("numeric"), equalTo(CoreValuesSourceType.NUMERIC));
        assertThat(CoreValuesSourceType.fromString("keyword"), equalTo(CoreValuesSourceType.KEYWORD));
        assertThat(CoreValuesSourceType.fromString("geopoint"), equalTo(CoreValuesSourceType.GEOPOINT));
        assertThat(CoreValuesSourceType.fromString("range"), equalTo(CoreValuesSourceType.RANGE));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CoreValuesSourceType.fromString("does_not_exist"));
        assertThat(
            e.getMessage(),
            equalTo("No enum constant org.elasticsearch.search.aggregations.support.CoreValuesSourceType.DOES_NOT_EXIST")
        );
        expectThrows(NullPointerException.class, () -> CoreValuesSourceType.fromString(null));
    }

    public void testDatePrepareRoundingWithNothing() throws IOException {
        withAggregationContext(dateMapperService(), List.of(), context -> {
            Rounding rounding = mock(Rounding.class);
            CoreValuesSourceType.DATE.getField(context.buildFieldContext("field"), null, context).roundingPreparer().apply(rounding);
            verify(rounding).prepareForUnknown();
        });
    }

    public void testDatePrepareRoundingWithDocs() throws IOException {
        long min = randomLongBetween(100000, 1000000);   // The minimum has to be fairly large or we might accidentally think its a year....
        long max = randomLongBetween(min + 1, 100000000000L);
        withAggregationContext(dateMapperService(), docsWithDatesBetween(min, max), context -> {
            Rounding rounding = mock(Rounding.class);
            CoreValuesSourceType.DATE.getField(context.buildFieldContext("field"), null, context).roundingPreparer().apply(rounding);
            verify(rounding).prepare(min, max);
        });
    }

    public void testDatePrepareRoundingWithQuery() throws IOException {
        long min = randomLongBetween(100000, 1000000);   // The minimum has to be fairly large or we might accidentally think its a year....
        long max = randomLongBetween(min + 10, 100000000000L);
        MapperService mapperService = dateMapperService();
        Query query = mapperService.fieldType("field")
            .rangeQuery(min, max, true, true, ShapeRelation.CONTAINS, null, null, createSearchExecutionContext(mapperService));
        withAggregationContext(null, mapperService, List.of(), query, context -> {
            Rounding rounding = mock(Rounding.class);
            CoreValuesSourceType.DATE.getField(context.buildFieldContext("field"), null, context).roundingPreparer().apply(rounding);
            verify(rounding).prepare(min, max);
        });
    }

    public void testDatePrepareRoundingWithDocAndQuery() throws IOException {
        long min = randomLongBetween(100000, 1000000); // The minimum has to be fairly large or we might accidentally think its a year....
        long minQuery, minDocs;
        if (randomBoolean()) {
            minQuery = min;
            minDocs = min - 1;
        } else {
            minQuery = min - 1;
            minDocs = min;
        }
        long max = randomLongBetween(min + 10, 100000000000L);
        long maxQuery, maxDocs;
        if (randomBoolean()) {
            maxQuery = max;
            maxDocs = max + 1;
        } else {
            maxQuery = max + 1;
            maxDocs = max;
        }
        MapperService mapperService = dateMapperService();
        Query query = mapperService.fieldType("field")
            .rangeQuery(minQuery, maxQuery, true, true, ShapeRelation.CONTAINS, null, null, createSearchExecutionContext(mapperService));
        withAggregationContext(null, mapperService, docsWithDatesBetween(minDocs, maxDocs), query, context -> {
            Rounding rounding = mock(Rounding.class);
            CoreValuesSourceType.DATE.getField(context.buildFieldContext("field"), null, context).roundingPreparer().apply(rounding);
            verify(rounding).prepare(min, max);
        });
    }

    private MapperService dateMapperService() throws IOException {
        return createMapperService(fieldMapping(b -> b.field("type", "date")));
    }

    private List<SourceToParse> docsWithDatesBetween(long min, long max) throws IOException {
        return List.of(source(b -> b.field("field", min)), source(b -> b.field("field", max)));
    }
}
