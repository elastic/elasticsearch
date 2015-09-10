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

import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PrefixQueryBuilderTests extends AbstractQueryTestCase<PrefixQueryBuilder> {

    @Override
    protected PrefixQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomBoolean() ? STRING_FIELD_NAME : randomAsciiOfLengthBetween(1, 10);
        String value = randomAsciiOfLengthBetween(1, 10);
        PrefixQueryBuilder query = new PrefixQueryBuilder(fieldName, value);

        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(PrefixQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) query;
        assertThat(prefixQuery.getPrefix().field(), equalTo(queryBuilder.fieldName()));
    }

    @Test
    public void testValidate() {
        PrefixQueryBuilder prefixQueryBuilder = new PrefixQueryBuilder("", "prefix");
        assertThat(prefixQueryBuilder.validate().validationErrors().size(), is(1));

        prefixQueryBuilder = new PrefixQueryBuilder("field", null);
        assertThat(prefixQueryBuilder.validate().validationErrors().size(), is(1));

        prefixQueryBuilder = new PrefixQueryBuilder("field", "prefix");
        assertNull(prefixQueryBuilder.validate());

        prefixQueryBuilder = new PrefixQueryBuilder(null, null);
        assertThat(prefixQueryBuilder.validate().validationErrors().size(), is(2));
    }
}