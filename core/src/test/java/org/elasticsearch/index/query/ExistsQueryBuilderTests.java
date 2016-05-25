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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class ExistsQueryBuilderTests extends AbstractQueryTestCase<ExistsQueryBuilder> {
    @Override
    protected ExistsQueryBuilder doCreateTestQueryBuilder() {
        String fieldPattern;
        if (randomBoolean()) {
            fieldPattern = randomFrom(MAPPED_FIELD_NAMES);
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
        return new ExistsQueryBuilder(fieldPattern);
    }

    @Override
    protected void doAssertLuceneQuery(ExistsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        String fieldPattern = queryBuilder.fieldName();
        Collection<String> fields = context.simpleMatchToIndexNames(fieldPattern);
        if (getCurrentTypes().length == 0) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            MatchNoDocsQuery matchNoDocsQuery = (MatchNoDocsQuery) query;
            assertThat(matchNoDocsQuery.toString(null), containsString("Missing types in \"exists\" query."));
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), instanceOf(BooleanQuery.class));
            BooleanQuery booleanQuery = (BooleanQuery) constantScoreQuery.getQuery();
            assertThat(booleanQuery.clauses().size(), equalTo(fields.size()));
            for (int i = 0; i < fields.size(); i++) {
                BooleanClause booleanClause = booleanQuery.clauses().get(i);
                assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
            }
        }
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new ExistsQueryBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new ExistsQueryBuilder(""));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"exists\" : {\n" +
                "    \"field\" : \"user\",\n" +
                "    \"boost\" : 42.0\n" +
                "  }\n" +
                "}";

        ExistsQueryBuilder parsed = (ExistsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 42.0, parsed.boost(), 0.0001);
        assertEquals(json, "user", parsed.fieldName());
    }
}
