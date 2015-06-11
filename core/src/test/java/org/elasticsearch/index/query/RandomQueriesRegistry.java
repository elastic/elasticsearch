package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class RandomQueriesRegistry {

    private final static Map<String, RandomQueryBuilder> registry;

    static {
        //order matters here for repeatable tests
        registry = new TreeMap<>();
        registry.put(MatchAllQueryBuilder.NAME, new RandomMatchAllQueryBuilder());
        registry.put(TermQueryBuilder.NAME, new RandomTermQueryBuilder());
        //span_term cannot go anywhere, only within span queries
        //registry.put(SpanTermQueryBuilder.NAME, new RandomSpanTermQueryBuilder());
        registry.put(RangeQueryBuilder.NAME, new RandomRangeQueryBuilder());
        registry.put(IdsQueryBuilder.NAME, new RandomIdsQueryBuilder());
        registry.put(LimitQueryBuilder.NAME, new RandomLimitQueryBuilder());
        registry.put(ExistsQueryBuilder.NAME, new RandomExistsQueryBuilder());
        registry.put(BoolQueryBuilder.NAME, new RandomBoolQueryBuilder());
    }

    public static QueryBuilder createRandomQuery(Random random) {
        return RandomPicks.randomFrom(random, registry.values()).newQueryBuilder(random);
    }
}
