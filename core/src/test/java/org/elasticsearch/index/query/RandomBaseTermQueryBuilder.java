package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import java.util.Random;

public abstract class RandomBaseTermQueryBuilder<QB extends BaseTermQueryBuilder> implements RandomQueryBuilder<QB> {

    @Override
    public final QB newQueryBuilder(Random random) {
        String fieldName = null;
        Object value;
        switch (RandomInts.randomIntBetween(random, 0, 3)) {
            case 0:
                if (random.nextBoolean()) {
                    fieldName = BaseQueryTestCase.BOOLEAN_FIELD_NAME;
                }
                value = random.nextBoolean();
                break;
            case 1:
                if (random.nextBoolean()) {
                    fieldName = BaseQueryTestCase.STRING_FIELD_NAME;
                }
                if (RandomInts.randomInt(random, 9) > 0) {
                    value = RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
                } else {
                    // generate unicode string in 10% of cases
                    value = RandomStrings.randomUnicodeOfLength(random, 10);
                }
                break;
            case 2:
                if (random.nextBoolean()) {
                    fieldName = BaseQueryTestCase.INT_FIELD_NAME;
                }
                value = RandomInts.randomInt(random, 10000);
                break;
            case 3:
                if (random.nextBoolean()) {
                    fieldName = BaseQueryTestCase.DOUBLE_FIELD_NAME;
                }
                value = random.nextDouble();
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (fieldName == null) {
            fieldName = RandomStrings.randomAsciiOfLengthBetween(random, 1, 10);
        }
        QB query = createQueryBuilder(fieldName, value);
        if (random.nextBoolean()) {
            query.boost(2.0f / RandomInts.randomIntBetween(random, 1, 20));
        }
        if (random.nextBoolean()) {
            query.queryName(RandomStrings.randomAsciiOfLengthBetween(random, 1, 10));
        }
        return query;
    }

    protected abstract QB createQueryBuilder(String fieldName, Object value);
}
