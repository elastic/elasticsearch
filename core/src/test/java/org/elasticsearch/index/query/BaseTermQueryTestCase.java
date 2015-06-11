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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Ignore
public abstract class BaseTermQueryTestCase<QB extends BaseTermQueryBuilder<QB>> extends BaseQueryTestCase<QB> {

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
                MappedFieldType mapper = context.fieldMapper(queryBuilder.fieldName());
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
