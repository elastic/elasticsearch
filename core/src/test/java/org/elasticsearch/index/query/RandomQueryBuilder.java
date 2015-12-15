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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import java.util.Random;

/**
 * Utility class for creating random QueryBuilders.
 * So far only leaf queries like {@link MatchAllQueryBuilder}, {@link TermQueryBuilder} or
 * {@link IdsQueryBuilder} are returned.
 */
public class RandomQueryBuilder {

    /**
     * Create a new query of a random type
     * @param r random seed
     * @return a random {@link QueryBuilder}
     */
    public static QueryBuilder createQuery(Random r) {
        switch (RandomInts.randomIntBetween(r, 0, 4)) {
            case 0:
                return new MatchAllQueryBuilderTests().createTestQueryBuilder();
            case 1:
                return new TermQueryBuilderTests().createTestQueryBuilder();
            case 2:
                return new IdsQueryBuilderTests().createTestQueryBuilder();
            case 3:
                return createMultiTermQuery(r);
            case 4:
                return EmptyQueryBuilder.PROTOTYPE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a new multi term query of a random type
     * @param r random seed
     * @return a random {@link MultiTermQueryBuilder}
     */
    public static MultiTermQueryBuilder createMultiTermQuery(Random r) {
        // for now, only use String Rangequeries for MultiTerm test, numeric and date makes little sense
        // see issue #12123 for discussion
        MultiTermQueryBuilder<?> multiTermQueryBuilder;
        switch(RandomInts.randomIntBetween(r, 0, 5)) {
            case 0:
                RangeQueryBuilder stringRangeQuery = new RangeQueryBuilder(AbstractQueryTestCase.STRING_FIELD_NAME);
                stringRangeQuery.from("a" + RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
                stringRangeQuery.to("z" + RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
                multiTermQueryBuilder = stringRangeQuery;
                break;
            case 1:
                RangeQueryBuilder numericRangeQuery = new RangeQueryBuilder(AbstractQueryTestCase.INT_FIELD_NAME);
                numericRangeQuery.from(RandomInts.randomIntBetween(r, 1, 100));
                numericRangeQuery.to(RandomInts.randomIntBetween(r, 101, 200));
                multiTermQueryBuilder = numericRangeQuery;
                break;
            case 2:
                multiTermQueryBuilder = new FuzzyQueryBuilder(AbstractQueryTestCase.INT_FIELD_NAME, RandomInts.randomInt(r, 1000));
                break;
            case 3:
                multiTermQueryBuilder = new FuzzyQueryBuilder(AbstractQueryTestCase.STRING_FIELD_NAME, RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
                break;
            case 4:
                multiTermQueryBuilder = new PrefixQueryBuilderTests().createTestQueryBuilder();
                break;
            case 5:
                multiTermQueryBuilder = new WildcardQueryBuilderTests().createTestQueryBuilder();
                break;
            default:
                throw new UnsupportedOperationException();
        }
        if (r.nextBoolean()) {
            multiTermQueryBuilder.boost(2.0f / RandomInts.randomIntBetween(r, 1, 20));
        }
        return multiTermQueryBuilder;
    }
}
