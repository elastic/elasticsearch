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

import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CommonTermsQueryBuilderTests extends BaseQueryTestCase<CommonTermsQueryBuilder> {

    @Override
    protected CommonTermsQueryBuilder doCreateTestQueryBuilder() {
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
            query.analyzer(randomAnalyzer());
        }

        if (randomBoolean()) {
            query.disableCoord(randomBoolean());
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(CommonTermsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery extendedCommonTermsQuery = (ExtendedCommonTermsQuery) query;
        assertThat(extendedCommonTermsQuery.getHighFreqMinimumNumberShouldMatchSpec(), equalTo(queryBuilder.highFreqMinimumShouldMatch()));
        assertThat(extendedCommonTermsQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo(queryBuilder.lowFreqMinimumShouldMatch()));
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
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        assertNull(builder.toQuery(context));
    }
}
