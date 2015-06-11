package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

import java.util.Random;

public class RandomLimitQueryBuilder implements RandomQueryBuilder<LimitQueryBuilder> {

    @Override
    public LimitQueryBuilder newQueryBuilder(Random random) {
        return new LimitQueryBuilder(RandomInts.randomIntBetween(random, 0, 20));
    }
}
