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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

import static org.hamcrest.Matchers.equalTo;

public class FieldMaskingSpanQueryBuilderTest extends BaseQueryTestCase<FieldMaskingSpanQueryBuilder> {

    @Override
    protected Query createExpectedQuery(FieldMaskingSpanQueryBuilder testQueryBuilder, QueryParseContext context) throws IOException {
        String fieldInQuery = testQueryBuilder.fieldName();
        MappedFieldType fieldType = context.fieldMapper(fieldInQuery);
        if (fieldType != null) {
            fieldInQuery = fieldType.names().indexName();
        }
        SpanQuery innerQuery = testQueryBuilder.innerQuery().toQuery(context);

        Query expectedQuery = new FieldMaskingSpanQuery(innerQuery, fieldInQuery);
        expectedQuery.setBoost(testQueryBuilder.boost());
        if (testQueryBuilder.queryName() != null) {
            context.addNamedQuery(testQueryBuilder.queryName(), expectedQuery);
        }
        return expectedQuery;
    }

    @Override
    protected void assertLuceneQuery(FieldMaskingSpanQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
    }

    @Override
    protected FieldMaskingSpanQueryBuilder createTestQueryBuilder() {
        String fieldName = null;
        if (randomBoolean()) {
            fieldName = randomFrom(mappedFieldNames);
        } else {
            fieldName = randomAsciiOfLengthBetween(1, 10);
        }
        SpanTermQueryBuilder innerQuery = new SpanTermQueryBuilderTest().createTestQueryBuilder();
        FieldMaskingSpanQueryBuilder query = new FieldMaskingSpanQueryBuilder(innerQuery, fieldName);
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
        }
        return query;
    }

    @Test
    public void testValidate() {
        FieldMaskingSpanQueryBuilder queryBuilder = new FieldMaskingSpanQueryBuilder(new SpanTermQueryBuilder("name", "value"), "fieldName");
        assertNull(queryBuilder.validate());

        queryBuilder = new FieldMaskingSpanQueryBuilder(null, "fieldName");
        assertThat(queryBuilder.validate().validationErrors().size(), is(1));

        queryBuilder = new FieldMaskingSpanQueryBuilder(null, "");
        assertThat(queryBuilder.validate().validationErrors().size(), is(2));

        queryBuilder = new FieldMaskingSpanQueryBuilder(null, null);
        assertThat(queryBuilder.validate().validationErrors().size(), is(2));
    }
}
