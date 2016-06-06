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

package org.elasticsearch.search.profile.query;

import org.apache.lucene.util.English;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CommonTermsQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.junit.Assert.assertTrue;


public class RandomQueryGenerator {
    public static QueryBuilder randomQueryBuilder(List<String> stringFields, List<String> numericFields, int numDocs, int depth) {
        assertTrue("Must supply at least one string field", stringFields.size() > 0);
        assertTrue("Must supply at least one numeric field", numericFields.size() > 0);

        // If depth is exhausted, or 50% of the time return a terminal
        // Helps limit ridiculously large compound queries
        if (depth == 0 || randomBoolean()) {
            return randomTerminalQuery(stringFields, numericFields, numDocs);
        }

        switch (randomIntBetween(0,5)) {
            case 0:
                return randomTerminalQuery(stringFields, numericFields, numDocs);
            case 1:
                return QueryBuilders.boolQuery().must(randomQueryBuilder(stringFields, numericFields, numDocs, depth -1))
                        .filter(randomQueryBuilder(stringFields, numericFields, numDocs, depth -1));
            case 2:
                return randomBoolQuery(stringFields, numericFields, numDocs, depth);
            case 3:
                // disabled for now because of https://issues.apache.org/jira/browse/LUCENE-6781
                //return randomBoostingQuery(stringFields, numericFields, numDocs, depth);
            case 4:
                return randomConstantScoreQuery(stringFields, numericFields, numDocs, depth);
            case 5:
                return randomDisMaxQuery(stringFields, numericFields, numDocs, depth);
            default:
                return randomTerminalQuery(stringFields, numericFields, numDocs);
        }
    }

    private static QueryBuilder randomTerminalQuery(List<String> stringFields, List<String> numericFields, int numDocs) {
        switch (randomIntBetween(0,6)) {
            case 0:
                return randomTermQuery(stringFields, numDocs);
            case 1:
                return randomTermsQuery(stringFields, numDocs);
            case 2:
                return randomRangeQuery(numericFields, numDocs);
            case 3:
                return QueryBuilders.matchAllQuery();
            case 4:
                return randomCommonTermsQuery(stringFields, numDocs);
            case 5:
                return randomFuzzyQuery(stringFields);
            case 6:
                return randomIDsQuery();
            default:
                return randomTermQuery(stringFields, numDocs);
        }
    }

    private static String randomQueryString(int max) {
        StringBuilder qsBuilder = new StringBuilder();

        for (int i = 0; i < max; i++) {
            qsBuilder.append(English.intToEnglish(randomInt(max)));
            qsBuilder.append(" ");
        }

        return qsBuilder.toString().trim();
    }

    private static String randomField(List<String> fields) {
        return fields.get(randomInt(fields.size() - 1));
    }



    private static QueryBuilder randomTermQuery(List<String> fields, int numDocs) {
        return QueryBuilders.termQuery(randomField(fields), randomQueryString(1));
    }

    private static QueryBuilder randomTermsQuery(List<String> fields, int numDocs) {
        int numTerms = randomInt(numDocs);
        ArrayList<String> terms = new ArrayList<>(numTerms);

        for (int i = 0; i < numTerms; i++) {
            terms.add(randomQueryString(1));
        }

        return QueryBuilders.termsQuery(randomField(fields), terms);
    }

    private static QueryBuilder randomRangeQuery(List<String> fields, int numDocs) {
        QueryBuilder q =  QueryBuilders.rangeQuery(randomField(fields));

        if (randomBoolean()) {
            ((RangeQueryBuilder)q).from(randomIntBetween(0, numDocs / 2 - 1));
        }
        if (randomBoolean()) {
            ((RangeQueryBuilder)q).to(randomIntBetween(numDocs / 2, numDocs));
        }

        return q;
    }

    private static QueryBuilder randomBoolQuery(List<String> stringFields, List<String> numericFields, int numDocs, int depth) {
        QueryBuilder q = QueryBuilders.boolQuery();
        int numClause = randomIntBetween(0,5);
        for (int i = 0; i < numClause; i++) {
            ((BoolQueryBuilder)q).must(randomQueryBuilder(stringFields, numericFields,numDocs, depth -1));
        }

        numClause = randomIntBetween(0,5);
        for (int i = 0; i < numClause; i++) {
            ((BoolQueryBuilder)q).should(randomQueryBuilder(stringFields, numericFields,numDocs, depth -1));
        }

        numClause = randomIntBetween(0,5);
        for (int i = 0; i < numClause; i++) {
            ((BoolQueryBuilder)q).mustNot(randomQueryBuilder(stringFields, numericFields, numDocs, depth -1));
        }

        return q;
    }

    private static QueryBuilder randomBoostingQuery(List<String> stringFields, List<String> numericFields, int numDocs, int depth) {
        return QueryBuilders.boostingQuery(
                randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1),
                randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1))
                .boost(randomFloat())
                .negativeBoost(randomFloat());
    }

    private static QueryBuilder randomConstantScoreQuery(List<String> stringFields, List<String> numericFields, int numDocs, int depth) {
        return QueryBuilders.constantScoreQuery(randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1));
    }

    private static QueryBuilder randomCommonTermsQuery(List<String> fields, int numDocs) {
        int numTerms = randomInt(numDocs);

        QueryBuilder q = QueryBuilders.commonTermsQuery(randomField(fields), randomQueryString(numTerms));
        if (randomBoolean()) {
            ((CommonTermsQueryBuilder)q).boost(randomFloat());
        }

        if (randomBoolean()) {
            ((CommonTermsQueryBuilder)q).cutoffFrequency(randomFloat());
        }

        if (randomBoolean()) {
            ((CommonTermsQueryBuilder)q).highFreqMinimumShouldMatch(Integer.toString(randomInt(numTerms)))
                    .highFreqOperator(randomBoolean() ? Operator.AND : Operator.OR);
        }

        if (randomBoolean()) {
            ((CommonTermsQueryBuilder)q).lowFreqMinimumShouldMatch(Integer.toString(randomInt(numTerms)))
                    .lowFreqOperator(randomBoolean() ? Operator.AND : Operator.OR);
        }

        return q;
    }

    @Deprecated
    private static QueryBuilder randomFuzzyQuery(List<String> fields) {

        QueryBuilder q = QueryBuilders.fuzzyQuery(randomField(fields), randomQueryString(1));

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).boost(randomFloat());
        }

        if (randomBoolean()) {
            switch (randomIntBetween(0, 4)) {
                case 0:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.AUTO);
                    break;
                case 1:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.ONE);
                    break;
                case 2:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.TWO);
                    break;
                case 3:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.ZERO);
                    break;
                case 4:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.fromEdits(randomIntBetween(0,2)));
                    break;
                default:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.AUTO);
                    break;
            }
        }

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).maxExpansions(Math.abs(randomInt()));
        }

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).prefixLength(Math.abs(randomInt()));
        }

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).transpositions(randomBoolean());
        }

        return q;
    }

    private static QueryBuilder randomDisMaxQuery(List<String> stringFields, List<String> numericFields, int numDocs, int depth) {
        QueryBuilder q =  QueryBuilders.disMaxQuery();

        int numClauses = randomIntBetween(1, 10);
        for (int i = 0; i < numClauses; i++) {
            ((DisMaxQueryBuilder)q).add(randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1));
        }

        if (randomBoolean()) {
            ((DisMaxQueryBuilder)q).boost(randomFloat());
        }

        if (randomBoolean()) {
            ((DisMaxQueryBuilder)q).tieBreaker(randomFloat());
        }

        return q;
    }

    private static QueryBuilder randomIDsQuery() {
        QueryBuilder q =  QueryBuilders.idsQuery();

        int numIDs = randomInt(100);
        for (int i = 0; i < numIDs; i++) {
            ((IdsQueryBuilder)q).addIds(String.valueOf(randomInt()));
        }

        if (randomBoolean()) {
            ((IdsQueryBuilder)q).boost(randomFloat());
        }

        return q;
    }
}
