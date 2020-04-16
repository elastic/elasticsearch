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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class MatchNoneQueryBuilderTests extends AbstractQueryTestCase<MatchNoneQueryBuilder> {

    @Override
    protected MatchNoneQueryBuilder doCreateTestQueryBuilder() {
        return new MatchNoneQueryBuilder();
    }

    @Override
    protected void doAssertLuceneQuery(MatchNoneQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"match_none\" : {\n" +
                "    \"boost\" : 1.2\n" +
                "  }\n" +
                "}";
        MatchNoneQueryBuilder parsed = (MatchNoneQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 1.2, parsed.boost(), 0.0001);
    }
}
