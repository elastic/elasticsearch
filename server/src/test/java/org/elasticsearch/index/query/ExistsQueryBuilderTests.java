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
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
            fieldPattern = randomAlphaOfLengthBetween(1, 10);
        }
        // also sometimes test wildcard patterns
        if (randomBoolean()) {
            if (randomBoolean()) {
                fieldPattern = fieldPattern + "*";
            }
        }
        return new ExistsQueryBuilder(fieldPattern);
    }

    @Override
    protected void doAssertLuceneQuery(ExistsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        String fieldPattern = queryBuilder.fieldName();
        Collection<String> fields = context.simpleMatchToIndexNames(fieldPattern);
        Collection<String> mappedFields = fields.stream().filter((field) -> context.getObjectMapper(field) != null
                || context.getMapperService().fullName(field) != null).collect(Collectors.toList());
        if (fields.size() == 1 && mappedFields.size() == 0) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            MatchNoDocsQuery matchNoDocsQuery = (MatchNoDocsQuery) query;
            assertThat(matchNoDocsQuery.toString(null),
                    containsString("No field \"" + fields.iterator().next() + "\" exists in mappings."));
        } else if (fields.size() == 1) {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            String field = expectedFieldName(fields.iterator().next());
            if (context.getObjectMapper(field) != null) {
                assertThat(constantScoreQuery.getQuery(), instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) constantScoreQuery.getQuery();
                List<String> childFields = new ArrayList<>();
                context.getObjectMapper(field).forEach(mapper -> childFields.add(mapper.name()));
                assertThat(booleanQuery.clauses().size(), equalTo(childFields.size()));
                for (int i = 0; i < childFields.size(); i++) {
                    BooleanClause booleanClause = booleanQuery.clauses().get(i);
                    assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
                }
            } else if (context.getMapperService().fullName(field).hasDocValues()) {
                assertThat(constantScoreQuery.getQuery(), instanceOf(DocValuesFieldExistsQuery.class));
                DocValuesFieldExistsQuery dvExistsQuery = (DocValuesFieldExistsQuery) constantScoreQuery.getQuery();
                assertEquals(field, dvExistsQuery.getField());
            } else if (context.getMapperService().fullName(field).omitNorms() == false) {
                assertThat(constantScoreQuery.getQuery(), instanceOf(NormsFieldExistsQuery.class));
                NormsFieldExistsQuery normsExistsQuery = (NormsFieldExistsQuery) constantScoreQuery.getQuery();
                assertEquals(field, normsExistsQuery.getField());
            } else {
                assertThat(constantScoreQuery.getQuery(), instanceOf(TermQuery.class));
                TermQuery termQuery = (TermQuery) constantScoreQuery.getQuery();
                assertEquals(field, termQuery.getTerm().text());
            }
        } else {
            assertThat(query, instanceOf(ConstantScoreQuery.class));
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            assertThat(constantScoreQuery.getQuery(), instanceOf(BooleanQuery.class));
            BooleanQuery booleanQuery = (BooleanQuery) constantScoreQuery.getQuery();
            assertThat(booleanQuery.clauses().size(), equalTo(mappedFields.size()));
            for (int i = 0; i < mappedFields.size(); i++) {
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
