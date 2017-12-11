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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.IntRange;
import org.apache.lucene.queries.BinaryDocValuesRangeQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class RangeFieldQueryStringQueryBuilderTests extends AbstractQueryTestCase<QueryStringQueryBuilder> {

    private static final String INTEGER_RANGE_FIELD_NAME = "mapped_int_range";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(MapperExtrasPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("doc", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("doc",
            INTEGER_RANGE_FIELD_NAME, "type=integer_range"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);

    }

    public void testIntegerRangeQuery() throws Exception {
        Query query = new QueryStringQueryBuilder(INTEGER_RANGE_FIELD_NAME + ":[-450 TO 45000]").toQuery(createShardContext());
        Query range = IntRange.newIntersectsQuery(INTEGER_RANGE_FIELD_NAME, new int[]{-450}, new int[]{45000});
        Query dv = RangeFieldMapper.RangeType.LONG.dvRangeQuery(INTEGER_RANGE_FIELD_NAME,
            BinaryDocValuesRangeQuery.QueryType.INTERSECTS, -450, 45000, true, true);
        assertEquals(new IndexOrDocValuesQuery(range, dv), query);
    }

    @Override
    protected QueryStringQueryBuilder doCreateTestQueryBuilder() {
        return new QueryStringQueryBuilder(INTEGER_RANGE_FIELD_NAME + ":[-450 TO 450]");
    }

    @Override
    protected void doAssertLuceneQuery(QueryStringQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, either(instanceOf(PointRangeQuery.class)).or(instanceOf(IndexOrDocValuesQuery.class)));
    }
}
