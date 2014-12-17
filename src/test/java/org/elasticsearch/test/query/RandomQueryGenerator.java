package org.elasticsearch.test.query;

import org.apache.lucene.util.English;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;

import java.util.ArrayList;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;


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
                return QueryBuilders.filteredQuery(randomQueryBuilder(stringFields, numericFields, numDocs, depth -1), randomFilterBuilder(numDocs, depth -1));
            case 2:
                return randomBoolQuery(stringFields, numericFields, numDocs, depth);
            case 3:
                return randomBoostingQuery(stringFields, numericFields, numDocs, depth);
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

    public static FilterBuilder randomFilterBuilder(int numDocs, int depth) {
        if (depth == 0) {
            return FilterBuilders.termFilter("field1", English.intToEnglish(randomInt(numDocs)));
        }
        FilterBuilder f;
        switch (randomIntBetween(0,7)) {
            case 0:
                f = FilterBuilders.matchAllFilter();
                break;
            case 1:
                f = FilterBuilders.termFilter("field1", English.intToEnglish(randomInt(numDocs)));
                break;
            case 2:
                f = FilterBuilders.termsFilter("field1", English.intToEnglish(randomInt(numDocs)),English.intToEnglish(randomInt(numDocs)));
                break;
            case 3:
                f = FilterBuilders.rangeFilter("field2").from(randomIntBetween(0,numDocs/2 - 1)).to(randomIntBetween(numDocs/2, numDocs));
                break;
            case 4:
                f = FilterBuilders.boolFilter();
                int numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolFilterBuilder)f).must(randomFilterBuilder(numDocs, depth -1));
                }
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolFilterBuilder)f).should(randomFilterBuilder(numDocs, depth -1));
                }
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolFilterBuilder)f).mustNot(randomFilterBuilder(numDocs, depth -1));
                }
                break;
            case 5:
                f = FilterBuilders.andFilter();
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((AndFilterBuilder)f).add(randomFilterBuilder(numDocs, depth -1));
                }
                break;
            case 6:
                f = FilterBuilders.orFilter();
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((OrFilterBuilder)f).add(randomFilterBuilder(numDocs, depth -1));
                }
                break;
            case 7:
                f = FilterBuilders.notFilter(randomFilterBuilder(numDocs, depth -1));
                break;
            default:
                f = FilterBuilders.termFilter("field1", English.intToEnglish(randomInt(numDocs)));
                break;
        }

        return f;
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
        return QueryBuilders.boostingQuery().boost(randomFloat())
                .positive(randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1))
                .negativeBoost(randomFloat())
                .negative(randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1));
    }

    private static QueryBuilder randomConstantScoreQuery(List<String> stringFields, List<String> numericFields, int numDocs, int depth) {
        if (randomBoolean()) {
            return QueryBuilders.constantScoreQuery(randomQueryBuilder(stringFields, numericFields, numDocs, depth - 1));
        } else {
            return QueryBuilders.constantScoreQuery(randomFilterBuilder(numDocs, depth - 1));
        }
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
                    .highFreqOperator(randomBoolean() ? CommonTermsQueryBuilder.Operator.AND : CommonTermsQueryBuilder.Operator.OR);
        }

        if (randomBoolean()) {
            ((CommonTermsQueryBuilder)q).lowFreqMinimumShouldMatch(Integer.toString(randomInt(numTerms)))
                    .lowFreqOperator(randomBoolean() ? CommonTermsQueryBuilder.Operator.AND : CommonTermsQueryBuilder.Operator.OR);
        }

        return q;
    }

    private static QueryBuilder randomFuzzyQuery(List<String> fields) {

        QueryBuilder q = QueryBuilders.fuzzyQuery(randomField(fields), randomQueryString(1));

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).boost(randomFloat());
        }

        if (randomBoolean()) {
            switch (randomIntBetween(0, 5)) {
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
                case 5:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.fromSimilarity(randomFloat()));
                    break;
                default:
                    ((FuzzyQueryBuilder)q).fuzziness(Fuzziness.AUTO);
                    break;
            }
        }

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).maxExpansions(randomInt());
        }

        if (randomBoolean()) {
            ((FuzzyQueryBuilder)q).prefixLength(randomInt());
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
            ((IdsQueryBuilder)q).addIds(English.longToEnglish(randomLong()));
        }

        if (randomBoolean()) {
            ((IdsQueryBuilder)q).boost(randomFloat());
        }

        return q;
    }
}
