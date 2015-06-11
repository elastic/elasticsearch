package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomRangeQueryBuilder implements RandomQueryBuilder<RangeQueryBuilder> {

    private static final List<String> TIMEZONE_IDS = new ArrayList<>(DateTimeZone.getAvailableIDs());

    @Override
    public RangeQueryBuilder newQueryBuilder(Random random) {
        RangeQueryBuilder query;
        // switch between numeric and date ranges
        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                // use mapped integer field for numeric range queries
                query = new RangeQueryBuilder(BaseQueryTestCase.INT_FIELD_NAME);
                query.from(RandomInts.randomIntBetween(random, 1, 100));
                query.to(RandomInts.randomIntBetween(random, 101, 200));
            } else {
                // use unmapped field for numeric range queries
                query = new RangeQueryBuilder(RandomStrings.randomAsciiOfLengthBetween(random, 1, 10));
                query.from(0.0 - random.nextDouble());
                query.to(random.nextDouble());
            }
        } else {
            // use mapped date field, using date string representation
            query = new RangeQueryBuilder(BaseQueryTestCase.DATE_FIELD_NAME);
            query.from(new DateTime(System.currentTimeMillis() - RandomInts.randomIntBetween(random, 0, 1000000)).toString());
            query.to(new DateTime(System.currentTimeMillis() + RandomInts.randomIntBetween(random, 0, 1000000)).toString());
            if (random.nextBoolean()) {
                query.timeZone(TIMEZONE_IDS.get(RandomInts.randomIntBetween(random, 0, TIMEZONE_IDS.size() - 1)));
            }
            if (random.nextBoolean()) {
                query.format("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
            }
        }
        query.includeLower(random.nextBoolean()).includeUpper(random.nextBoolean());
        if (random.nextBoolean()) {
            query.boost(2.0f / RandomInts.randomIntBetween(random, 1, 20));
        }
        if (random.nextBoolean()) {
            query.queryName(RandomStrings.randomAsciiOfLengthBetween(random, 1, 10));
        }

        if (random.nextBoolean()) {
            query.from(null);
        }
        if (random.nextBoolean()) {
            query.to(null);
        }
        return query;
    }
}
