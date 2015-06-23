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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CommonTermsQueryBuilderTest extends BaseQueryTestCase<CommonTermsQueryBuilder> {

    @Override
    protected CommonTermsQueryBuilder createTestQueryBuilder() {
        CommonTermsQueryBuilder query;
        
        // mapped or unmapped field
        String text = randomAsciiOfLengthBetween(1, 10);
        if (randomBoolean()) {
            query = new CommonTermsQueryBuilder(STRING_FIELD_NAME, text);
        } else {
            query = new CommonTermsQueryBuilder(randomAsciiOfLengthBetween(1, 10), text);
        }
        
        if (randomBoolean()) {
            query.cutoffFrequency((float) randomIntBetween(1, 10));
        }

        if (randomBoolean()) {
            query.lowFreqOperator(randomFrom(Operator.values()));
        }
            
        // number of low frequency terms that must match
        if (randomBoolean()) {
            query.lowFreqMinimumShouldMatch("" + randomIntBetween(1, 5));
        }

        if (randomBoolean()) {
            query.highFreqOperator(randomFrom(Operator.values()));
        }

        // number of high frequency terms that must match
        if (randomBoolean()) {
            query.highFreqMinimumShouldMatch("" + randomIntBetween(1, 5));
        }
        
        if (randomBoolean()) {
            query.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }
        
        if (randomBoolean()) {
            query.disableCoord(randomBoolean());
        }
        
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
        }
        
        return query;
    }

    @Override
    protected Query createExpectedQuery(CommonTermsQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        String fieldName = queryBuilder.fieldName();
        Analyzer analyzer = context.mapperService().searchAnalyzer();

        // handle mapped field
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            fieldName = fieldType.names().indexName();
            analyzer = context.getSearchAnalyzer(fieldType);
        }

        // handle specified analyzer
        if (queryBuilder.analyzer() != null) {
            analyzer = context.analysisService().analyzer(queryBuilder.analyzer());
        }
        
        Occur highFreqOccur = queryBuilder.highFreqOperator().toBooleanClauseOccur();
        Occur lowFreqOccur = queryBuilder.lowFreqOperator().toBooleanClauseOccur();

        ExtendedCommonTermsQuery expectedQuery = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, queryBuilder.cutoffFrequency(), 
                queryBuilder.disableCoord(), fieldType);
        CommonTermsQueryBuilder.parseQueryString(expectedQuery, queryBuilder.text(), fieldName, analyzer, 
                queryBuilder.lowFreqMinimumShouldMatch(), queryBuilder.highFreqMinimumShouldMatch());

        expectedQuery.setBoost(queryBuilder.boost());
        return expectedQuery;
    }

    @Override
    protected void assertLuceneQuery(CommonTermsQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
    }

    @Test
    public void testValidate() {
        CommonTermsQueryBuilder commonTermsQueryBuilder = new CommonTermsQueryBuilder("", "text");
        assertThat(commonTermsQueryBuilder.validate().validationErrors().size(), is(1));

        commonTermsQueryBuilder = new CommonTermsQueryBuilder("field", null);
        assertThat(commonTermsQueryBuilder.validate().validationErrors().size(), is(1));

        commonTermsQueryBuilder = new CommonTermsQueryBuilder("field", "text");
        assertNull(commonTermsQueryBuilder.validate());
    }

    @Test
    public void testNoTermsFromQueryString() throws IOException {
        CommonTermsQueryBuilder builder = new CommonTermsQueryBuilder(STRING_FIELD_NAME, "");
        assertNull(builder.toQuery(createContext()));
    }
}
