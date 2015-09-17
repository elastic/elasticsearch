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
        // Prefix / Fuzzy / RegEx / Wildcard can go here later once refactored and they have random query generators
        RangeQueryBuilder query = new RangeQueryBuilder(AbstractQueryTestCase.STRING_FIELD_NAME);
        query.from("a" + RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
        query.to("z" + RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
        return query;
    }
}
