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

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;

public class TermsQueryBuilderTypedTests extends AbstractQueryTestCase<TermsQueryBuilder> {
    private List<Object> randomTerms;
    private String termsPath;

    @Before
    public void randomTerms() {
        List<Object> randomTerms = new ArrayList<>();
        String[] strings = generateRandomStringArray(10, 10, false, true);
        for (String string : strings) {
            randomTerms.add(string);
            if (rarely()) {
                randomTerms.add(null);
            }
        }
        this.randomTerms = randomTerms;
        this.termsPath = randomAlphaOfLength(10).replace('.', '_');
    }

    @Override
    protected TermsQueryBuilder doCreateTestQueryBuilder() {
        TermsQueryBuilder query = new TermsQueryBuilder(
            randomBoolean() ? randomAlphaOfLengthBetween(1,10) : STRING_FIELD_NAME, randomTermsLookup());
        return query;
    }

    private TermsLookup randomTermsLookup() {
        return new TermsLookup(
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                termsPath
            ).routing(randomBoolean() ? randomAlphaOfLength(10) : null);
    }

    @Override
    protected void doAssertLuceneQuery(TermsQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        if (queryBuilder.termsLookup() != null && randomTerms.size() == 0){
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            MatchNoDocsQuery matchNoDocsQuery = (MatchNoDocsQuery) query;
            assertThat(matchNoDocsQuery.toString(), containsString("No terms supplied for \"terms\" query."));
        } else {
            assertThat(query, either(instanceOf(TermInSetQuery.class))
                    .or(instanceOf(PointInSetQuery.class))
                    .or(instanceOf(ConstantScoreQuery.class)));
            if (query instanceof ConstantScoreQuery) {
                assertThat(((ConstantScoreQuery) query).getQuery(), instanceOf(BooleanQuery.class));
            }
            String fieldName = expectedFieldName(queryBuilder.fieldName());
            TermInSetQuery expected = new TermInSetQuery(fieldName,
                    randomTerms.stream().filter(Objects::nonNull).map(Object::toString).map(BytesRef::new).collect(Collectors.toList()));
            assertEquals(expected, query);
        }
    }

    @Override
    public GetResponse executeGet(GetRequest getRequest) {
        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.array(termsPath, randomTerms.toArray(new Object[randomTerms.size()]));
            builder.endObject();
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(getRequest.index(), getRequest.type(), getRequest.id(), 0, 1, 0, true,
            new BytesArray(json), null));
    }

    @Override
    public void testMustRewrite() throws IOException {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(STRING_FIELD_NAME, randomTermsLookup());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> termsQueryBuilder.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());
    }
    
    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testUnknownField() throws IOException {
        super.testUnknownField();
        assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
    }

    @Override
    public void testValidOutput() throws IOException {
        super.testValidOutput();
        assertWarnings(QueryShardContext.TYPES_DEPRECATION_MESSAGE);
    }
}

