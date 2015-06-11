package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.cluster.metadata.MetaData;

import java.util.Random;

public class RandomIdsQueryBuilder implements RandomQueryBuilder<IdsQueryBuilder> {

    @Override
    public IdsQueryBuilder newQueryBuilder(Random random) {
        String[] types;
        if (BaseQueryTestCase.getCurrentTypes().length > 0 && random.nextBoolean()) {
            int numberOfTypes = RandomInts.randomIntBetween(random, 1, BaseQueryTestCase.getCurrentTypes().length);
            types = new String[numberOfTypes];
            for (int i = 0; i < numberOfTypes; i++) {
                if (RandomInts.randomInt(random, 9) > 0) {
                    types[i] = RandomPicks.randomFrom(random, BaseQueryTestCase.getCurrentTypes());
                } else {
                    types[i] = RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
                }
            }
        } else {
            if (random.nextBoolean()) {
                types = new String[]{MetaData.ALL};
            } else {
                types = new String[0];
            }
        }
        int numberOfIds = RandomInts.randomIntBetween(random, 0, 10);
        String[] ids = new String[numberOfIds];
        for (int i = 0; i < numberOfIds; i++) {
            ids[i] = RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
        }
        IdsQueryBuilder query;
        if (types.length > 0 || random.nextBoolean()) {
            query = new IdsQueryBuilder(types);
            query.addIds(ids);
        } else {
            query = new IdsQueryBuilder();
            query.addIds(ids);
        }
        if (random.nextBoolean()) {
            query.boost(2.0f / RandomInts.randomIntBetween(random, 1, 20));
        }
        if (random.nextBoolean()) {
            query.queryName(RandomStrings.randomAsciiOfLengthBetween(random, 1, 10));
        }
        return query;
    }
}
