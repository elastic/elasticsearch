package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import java.util.Random;

public class RandomBoolQueryBuilder implements RandomQueryBuilder<BoolQueryBuilder> {

    @Override
    public BoolQueryBuilder newQueryBuilder(Random random) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        if (random.nextBoolean()) {
            query.boost(2.0f / RandomInts.randomIntBetween(random, 1, 20));
        }
        if (random.nextBoolean()) {
            query.adjustPureNegative(random.nextBoolean());
        }
        if (random.nextBoolean()) {
            query.disableCoord(random.nextBoolean());
        }
        if (random.nextBoolean()) {
            query.minimumNumberShouldMatch(RandomInts.randomIntBetween(random, 1, 10));
        }
        int mustClauses = RandomInts.randomIntBetween(random, 0, 3);
        for (int i = 0; i < mustClauses; i++) {
            query.must(RandomQueriesRegistry.createRandomQuery(random));
        }
        int mustNotClauses = RandomInts.randomIntBetween(random, 0, 3);
        for (int i = 0; i < mustNotClauses; i++) {
            query.mustNot(RandomQueriesRegistry.createRandomQuery(random));
        }
        int shouldClauses = RandomInts.randomIntBetween(random, 0, 3);
        for (int i = 0; i < shouldClauses; i++) {
            query.should(RandomQueriesRegistry.createRandomQuery(random));
        }
        int filterClauses = RandomInts.randomIntBetween(random, 0, 3);
        for (int i = 0; i < filterClauses; i++) {
            query.filter(RandomQueriesRegistry.createRandomQuery(random));
        }
        if (random.nextBoolean()) {
            query.queryName(RandomStrings.randomUnicodeOfLengthBetween(random, 3, 15));
        }
        return query;
    }
}
