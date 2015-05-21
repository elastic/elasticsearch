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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.FieldMapper;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Ignore
public abstract class BaseTermQueryTestCase<QB extends BaseTermQueryBuilder<QB>> extends BaseQueryTestCase<QB> {
    
    protected final QB createTestQueryBuilder() {
        String fieldName = null;
        Object value;
        switch (randomIntBetween(0, 3)) {
            case 0:
                if (randomBoolean()) {
                    fieldName = BOOLEAN_FIELD_NAME;
                }
                value = randomBoolean();
                break;
            case 1:
                if (randomBoolean()) {
                    fieldName = STRING_FIELD_NAME;
                }
                if (frequently()) {
                    value = randomAsciiOfLengthBetween(1, 10);
                } else {
                    // generate unicode string in 10% of cases
                    value = randomUnicodeOfLength(10);
                }
                break;
            case 2:
                if (randomBoolean()) {
                    fieldName = INT_FIELD_NAME;
                }
                value = randomInt(10000);
                break;
            case 3:
                if (randomBoolean()) {
                    fieldName = DOUBLE_FIELD_NAME;
                }
                value = randomDouble();
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (fieldName == null) {
            fieldName = randomAsciiOfLengthBetween(1, 10);
        }
        QB query = createQueryBuilder(fieldName, value);
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
        }
        return query;
    }

    protected abstract QB createQueryBuilder(String fieldName, Object value);

    @Test
    public void testValidate() throws QueryParsingException {
        
        QB queryBuilder = createQueryBuilder("all", "good");
        assertNull(queryBuilder.validate());

        queryBuilder = createQueryBuilder(null, "Term");
        assertNotNull(queryBuilder.validate());
        assertThat(queryBuilder.validate().validationErrors().size(), is(1));

        queryBuilder = createQueryBuilder("", "Term");
        assertNotNull(queryBuilder.validate());
        assertThat(queryBuilder.validate().validationErrors().size(), is(1));

        queryBuilder = createQueryBuilder("", null);
        assertNotNull(queryBuilder.validate());
        assertThat(queryBuilder.validate().validationErrors().size(), is(2));
    }

    @Override
    protected Query createExpectedQuery(QB queryBuilder, QueryParseContext context) {
        BytesRef value = null;
        if (getCurrentTypes().length > 0) {
            if (queryBuilder.fieldName().equals(BOOLEAN_FIELD_NAME) || queryBuilder.fieldName().equals(INT_FIELD_NAME) || queryBuilder.fieldName().equals(DOUBLE_FIELD_NAME)) {
                FieldMapper mapper = context.fieldMapper(queryBuilder.fieldName());
                value = mapper.indexedValueForSearch(queryBuilder.value);
            }
        }
        if (value == null) {
            value = BytesRefs.toBytesRef(queryBuilder.value);
        }
        Query termQuery = createLuceneTermQuery(new Term(queryBuilder.fieldName(), value));
        termQuery.setBoost(queryBuilder.boost());
        return termQuery;
    }

    protected abstract Query createLuceneTermQuery(Term term);

    @Override
    protected void assertLuceneQuery(QB queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
    }
}
