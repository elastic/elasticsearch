package org.elasticsearch.common.util;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ExactBloomFilterTests extends AbstractWireSerializingTestCase<ExactBloomFilter> {

    @Override
    protected ExactBloomFilter createTestInstance() {
        ExactBloomFilter bloom = new ExactBloomFilter(randomIntBetween(1, 100000000),
            ((float)randomIntBetween(1, 50)) / 100.0, randomNonNegativeLong());

        int num = randomIntBetween(0, 10);
        for (int i = 0; i < num; i++) {
            bloom.put(randomLong());
        }

        return bloom;
    }

    @Override
    protected Writeable.Reader<ExactBloomFilter> instanceReader() {
        return ExactBloomFilter::new;
    }

    @Override
    protected ExactBloomFilter mutateInstance(ExactBloomFilter instance) {
        ExactBloomFilter newInstance = new ExactBloomFilter(instance);
        int num = randomIntBetween(1, 10);
        for (int i = 0; i < num; i++) {
            newInstance.put(randomLong());
        }
        return newInstance;
    }

    public void testExact() {
        long threshold = randomLongBetween(1000, 10000);
        ExactBloomFilter bloom = new ExactBloomFilter(1000000, 0.03, threshold);

        int size = 0;
        Set<Long> values = new HashSet<>();
        Set<MurmurHash3.Hash128> hashed = new HashSet<>(values.size());
        while (size < threshold - 100) {
            long value = randomLong();
            bloom.put(value);
            boolean newValue = values.add(value);
            if (newValue) {
                byte[] bytes = Numbers.longToBytes(value);
                MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
                hashed.add(hash128);

                size += 16;
            }
        }
        assertThat(bloom.hashedValues.size(), equalTo(hashed.size()));
        assertThat(bloom.hashedValues, equalTo(hashed));

        for (Long value : values) {
            assertThat(bloom.mightContain(value), equalTo(true));
        }
    }

    public void testConvert() {
        long threshold = randomLongBetween(1000, 10000);
        ExactBloomFilter bloom = new ExactBloomFilter(1000000, 0.03, threshold);

        int size = 0;
        Set<Long> values = new HashSet<>();
        while (size < threshold + 100) {
            long value = randomLong();
            bloom.put(value);
            boolean newValue = values.add(value);
            if (newValue) {
                size += 16;
            }
        }
        assertThat(bloom.hashedValues, empty());
        assertThat(bloom.bits.bitSize(), greaterThan(0L));
    }

}
