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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

public class ExistsQueryBuilderTest extends BaseQueryTestCase<ExistsQueryBuilder> {

    private Collection<String> getFieldNamePattern(String fieldName, QueryParseContext context) {
        if (getCurrentTypes().length > 0 && fieldName.equals(BaseQueryTestCase.OBJECT_FIELD_NAME)) {
            // "object" field has two inner fields (age, price), so if query hits that field, we
            // extend field name with wildcard to match both nested fields. This is similar to what
            // is done internally in ExistsQueryBuilder.toQuery()
            fieldName = fieldName + ".*";
        }
        return context.simpleMatchToIndexNames(fieldName);
    }

    @Override
    protected Query createExpectedQuery(ExistsQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType)context.mapperService().fullName(FieldNamesFieldMapper.NAME);
        Collection<String> fields = getFieldNamePattern(queryBuilder.name(), context);

        if (fields.isEmpty() || fieldNamesFieldType == null) {
            return Queries.newMatchNoDocsQuery();
        }

        BooleanQuery boolFilter = new BooleanQuery();
        for (String field : fields) {
            if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
                boolFilter.add(fieldNamesFieldType.termQuery(field, context), BooleanClause.Occur.SHOULD);
            } else {
                MappedFieldType fieldType = context.fieldMapper(field);
                if (fieldType == null) {
                    boolFilter.add(new TermRangeQuery(field, null, null, true, true), BooleanClause.Occur.SHOULD);
                } else {
                    boolFilter.add(fieldType.rangeQuery(null, null, true, true, context), BooleanClause.Occur.SHOULD);
                }
            }
        }
        return new ConstantScoreQuery(boolFilter);
    }

    @Override
    protected void assertLuceneQuery(ExistsQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            Collection<String> fields = getFieldNamePattern(queryBuilder.name(), context);

            if (fields.isEmpty()) {
                assertNull(namedQuery);
            } else {
                query = ((ConstantScoreQuery) query).getQuery();
                assertThat(namedQuery, equalTo(query));
            }
        }
    }

    @Override
    protected ExistsQueryBuilder createTestQueryBuilder() {
        String fieldPattern;
        if (randomBoolean()) {
            fieldPattern = randomFrom(mappedFieldNames);
        } else {
            fieldPattern = randomAsciiOfLengthBetween(1, 10);
        }
        // also sometimes test wildcard patterns
        if (randomBoolean()) {
            if (randomBoolean()) {
                fieldPattern = fieldPattern + "*";
            } else {
                fieldPattern = MetaData.ALL;
            }
        }
        ExistsQueryBuilder query = new ExistsQueryBuilder(fieldPattern);

        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
        }
        return query;
    }
}
