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
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;

public class MatchAllQueryBuilderTests extends AbstractQueryTestCase<MatchAllQueryBuilder> {

    @Override
    protected MatchAllQueryBuilder doCreateTestQueryBuilder() {
        return new MatchAllQueryBuilder();
    }

    @Override
    protected Map<String, MatchAllQueryBuilder> getAlternateVersions() {
        Map<String, MatchAllQueryBuilder> alternateVersions = new HashMap<>();
        String queryAsString = "{\n" +
                "    \"match_all\": []\n" +
                "}";
        alternateVersions.put(queryAsString, new MatchAllQueryBuilder());
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchAllQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"match_all\" : {\n" +
                "    \"boost\" : 1.2\n" +
                "  }\n" +
                "}";
        MatchAllQueryBuilder parsed = (MatchAllQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 1.2, parsed.boost(), 0.0001);
    }
}
