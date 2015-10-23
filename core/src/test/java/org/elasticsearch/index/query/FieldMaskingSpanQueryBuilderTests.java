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
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class FieldMaskingSpanQueryBuilderTests extends AbstractQueryTestCase<FieldMaskingSpanQueryBuilder> {
    @Override
    protected FieldMaskingSpanQueryBuilder doCreateTestQueryBuilder() {
        String fieldName;
        if (randomBoolean()) {
            fieldName = randomFrom(MAPPED_FIELD_NAMES);
        } else {
            fieldName = randomAsciiOfLengthBetween(1, 10);
        }
        SpanTermQueryBuilder innerQuery = new SpanTermQueryBuilderTests().createTestQueryBuilder();
        return new FieldMaskingSpanQueryBuilder(innerQuery, fieldName);
    }

    @Override
    protected void doAssertLuceneQuery(FieldMaskingSpanQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        String fieldInQuery = queryBuilder.fieldName();
        MappedFieldType fieldType = context.fieldMapper(fieldInQuery);
        if (fieldType != null) {
            fieldInQuery = fieldType.names().indexName();
        }
        assertThat(query, instanceOf(FieldMaskingSpanQuery.class));
        FieldMaskingSpanQuery fieldMaskingSpanQuery = (FieldMaskingSpanQuery) query;
        assertThat(fieldMaskingSpanQuery.getField(), equalTo(fieldInQuery));
        assertThat(fieldMaskingSpanQuery.getMaskedQuery(), equalTo(queryBuilder.innerQuery().toQuery(context)));
    }

    public void testIllegalArguments() {
        try {
            new FieldMaskingSpanQueryBuilder(null, "maskedField");
            fail("must be non null");
        } catch (IllegalArgumentException e) {
            // okay
        }

        try {
            SpanQueryBuilder span = new SpanTermQueryBuilder("name", "value");
            if (randomBoolean()) {
                new FieldMaskingSpanQueryBuilder(span, null);
            } else {
                new FieldMaskingSpanQueryBuilder(span, "");
            }
            fail("must be non null");
        } catch (IllegalArgumentException e) {
            // okay
        }
    }
}
