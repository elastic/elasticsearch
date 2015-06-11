package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.cluster.metadata.MetaData;

import java.util.Random;

public class RandomExistsQueryBuilder implements RandomQueryBuilder<ExistsQueryBuilder> {

    @Override
    public ExistsQueryBuilder newQueryBuilder(Random random) {
        String fieldPattern;
        if (random.nextBoolean()) {
            fieldPattern = RandomPicks.randomFrom(random, BaseQueryTestCase.MAPPED_FIELD_NAMES);
        } else {
            fieldPattern = RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
        }
        // also sometimes test wildcard patterns
        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                fieldPattern = fieldPattern + "*";
            } else {
                fieldPattern = MetaData.ALL;
            }
        }
        ExistsQueryBuilder query = new ExistsQueryBuilder(fieldPattern);

        if (random.nextBoolean()) {
            query.queryName(RandomStrings.randomAsciiOfLengthBetween(random, 1, 10));
        }
        return query;
    }
}
