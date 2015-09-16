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

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.Fuzziness;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class FuzzyQueryBuilderTests extends AbstractQueryTestCase<FuzzyQueryBuilder> {

    @Override
    protected FuzzyQueryBuilder doCreateTestQueryBuilder() {
        Tuple<String, Object> fieldAndValue = getRandomFieldNameAndValue();
        FuzzyQueryBuilder query = new FuzzyQueryBuilder(fieldAndValue.v1(), fieldAndValue.v2());
        if (randomBoolean()) {
            query.fuzziness(randomFuzziness(query.fieldName()));
        }
        if (randomBoolean()) {
            query.prefixLength(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            query.maxExpansions(randomIntBetween(1, 10));
        }
        if (randomBoolean()) {
            query.transpositions(randomBoolean());
        }
        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(FuzzyQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (isNumericFieldName(queryBuilder.fieldName()) || queryBuilder.fieldName().equals(DATE_FIELD_NAME)) {
            assertThat(query, instanceOf(NumericRangeQuery.class));
        } else {
            assertThat(query, instanceOf(FuzzyQuery.class));
        }
    }

    @Test
    public void testIllegalArguments() {
        try {
            new FuzzyQueryBuilder(null, "text");
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new FuzzyQueryBuilder("", "text");
            fail("must not be empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new FuzzyQueryBuilder("field", null);
            fail("must not be null");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testUnsupportedFuzzinessForStringType() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);

        FuzzyQueryBuilder fuzzyQueryBuilder = new FuzzyQueryBuilder(STRING_FIELD_NAME, "text");
        fuzzyQueryBuilder.fuzziness(Fuzziness.build(randomFrom("a string which is not auto", "3h", "200s")));

        try {
            fuzzyQueryBuilder.toQuery(context);
            fail("should have failed with NumberFormatException");
        } catch (NumberFormatException e) {
            assertThat(e.getMessage(), Matchers.containsString("For input string"));
        }
    }
}
