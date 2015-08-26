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
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class MissingQueryBuilderTest extends BaseQueryTestCase<MissingQueryBuilder> {

    @Override
    protected MissingQueryBuilder doCreateTestQueryBuilder() {
        MissingQueryBuilder query  = new MissingQueryBuilder(randomBoolean() ? randomFrom(MAPPED_FIELD_NAMES) : randomAsciiOfLengthBetween(1, 10));
        if (randomBoolean()) {
            query.nullValue(randomBoolean());
        }
        if (randomBoolean()) {
            query.existence(randomBoolean());
        }
        // cannot set both to false
        if ((query.nullValue() == false) && (query.existence() == false)) {
            query.existence(!query.existence());
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(MissingQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        //too many mapping dependent cases to test, we don't want to end up duplication the toQuery method
    }

    @Test
    public void testValidate() {
        MissingQueryBuilder missingQueryBuilder = new MissingQueryBuilder("");
        assertThat(missingQueryBuilder.validate().validationErrors().size(), is(1));

        missingQueryBuilder = new MissingQueryBuilder(null);
        assertThat(missingQueryBuilder.validate().validationErrors().size(), is(1));

        missingQueryBuilder = new MissingQueryBuilder("field").existence(false).nullValue(false);
        assertThat(missingQueryBuilder.validate().validationErrors().size(), is(1));

        missingQueryBuilder = new MissingQueryBuilder("field");
        assertNull(missingQueryBuilder.validate());
    }

    @Test(expected = QueryShardException.class)
    public void testBothNullValueAndExistenceFalse() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        MissingQueryBuilder.newFilter(context, "field", false, false);
    }
}
