/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

public class EsqlSearchExecutionContextTests extends MapperServiceTestCase {
    public void testSetTimeRangeFilterFromMillisPutsThreadContextTransient() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        SearchExecutionContext base = createSearchExecutionContext(mapperService);
        EsqlSearchExecutionContext ctx = new EsqlSearchExecutionContext(base, QueryWarnings.NOOP, threadContext);

        assertNull(threadContext.getTransient(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FROM_ATTRIBUTE));

        ctx.setTimeRangeFilterFromMillis("not_a_timestamp", 0L, DateFieldMapper.Resolution.MILLISECONDS);
        assertNull(threadContext.getTransient(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FROM_ATTRIBUTE));

        long fromMillis = base.nowInMillis() - TimeValue.timeValueMinutes(10).millis();
        ctx.setTimeRangeFilterFromMillis(DataStream.TIMESTAMP_FIELD_NAME, fromMillis, DateFieldMapper.Resolution.MILLISECONDS);
        assertEquals("15_minutes", threadContext.getTransient(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FROM_ATTRIBUTE));
    }
}
