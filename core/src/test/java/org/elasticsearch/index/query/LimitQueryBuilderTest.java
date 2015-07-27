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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class LimitQueryBuilderTest extends BaseQueryTestCase<LimitQueryBuilder> {

    /**
     * @return a LimitQueryBuilder with random limit between 0 and 20
     */
    @Override
    protected LimitQueryBuilder doCreateTestQueryBuilder() {
        return new LimitQueryBuilder(randomIntBetween(0, 20));
    }

    @Override
    protected void doAssertLuceneQuery(LimitQueryBuilder queryBuilder, Query query, QueryParseContext context) throws IOException {
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }
}
