package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

import java.util.Random;

public class RandomMatchAllQueryBuilder implements RandomQueryBuilder<MatchAllQueryBuilder> {

    @Override
    public MatchAllQueryBuilder newQueryBuilder(Random random) {
        MatchAllQueryBuilder query = new MatchAllQueryBuilder();
        if (random.nextBoolean()) {
            query.boost(2.0f / RandomInts.randomIntBetween(random, 1, 20));
        }
        return query;
    }
}
