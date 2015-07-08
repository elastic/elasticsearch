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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.junit.Test;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class FilteredQueryBuilderTest extends BaseQueryTestCase<FilteredQueryBuilder> {

    @Override
    protected FilteredQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder queryBuilder = RandomQueryBuilder.createQuery(random());
        QueryBuilder filterBuilder = RandomQueryBuilder.createQuery(random());

        FilteredQueryBuilder query = new FilteredQueryBuilder(queryBuilder, filterBuilder);
        return query;
    }

    @Override
    protected Query doCreateExpectedQuery(FilteredQueryBuilder qb, QueryParseContext context) throws IOException {
        Query query = qb.query().toQuery(context);
        Query filter = qb.filter().toQuery(context);

        if (query == null) {
            return null;
        }

        Query result;
        if (filter == null || Queries.isConstantMatchAllQuery(filter)) {
            result = qb.query().toQuery(context);
        } else if (Queries.isConstantMatchAllQuery(query)) {
            result = new ConstantScoreQuery(filter);
        } else {
            result = Queries.filtered(qb.query().toQuery(context), filter);
        }
        result.setBoost(qb.boost());
        return result;
    }

    @Test
    public void testValidation() {
        QueryBuilder valid = RandomQueryBuilder.createQuery(random());
        QueryBuilder invalid = RandomQueryBuilder.createInvalidQuery(random());

        // invalid cases
        FilteredQueryBuilder qb = new FilteredQueryBuilder(invalid);
        QueryValidationException result = qb.validate();
        assertNotNull(result);
        assertEquals(1, result.validationErrors().size());

        qb = new FilteredQueryBuilder(valid, invalid);
        result = qb.validate();
        assertNotNull(result);
        assertEquals(1, result.validationErrors().size());

        qb = new FilteredQueryBuilder(invalid, valid);
        result = qb.validate();
        assertNotNull(result);
        assertEquals(1, result.validationErrors().size());

        qb = new FilteredQueryBuilder(invalid, invalid);
        result = qb.validate();
        assertNotNull(result);
        assertEquals(2, result.validationErrors().size());

        // valid cases
        qb = new FilteredQueryBuilder(valid);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(null);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(null, valid);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(valid, null);
        assertNull(qb.validate());

        qb = new FilteredQueryBuilder(valid, valid);
        assertNull(qb.validate());
    }

}
