/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLengthBetween;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLongBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class InternalCompositeTests extends InternalMultiBucketAggregationTestCase<InternalComposite> {
    private List<String> sourceNames;
    private int[] reverseMuls;
    private int[] formats;
    private int size;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        int numFields = randomIntBetween(1, 10);
        size = randomNumberOfBuckets();
        sourceNames = new ArrayList<>();
        reverseMuls = new int[numFields];
        formats = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            sourceNames.add("field_" + i);
            reverseMuls[i] = randomBoolean() ? 1 : -1;
            formats[i] = randomIntBetween(0, 2);
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        sourceNames= null;
        reverseMuls = null;
        formats = null;
    }

    @Override
    protected Writeable.Reader<InternalComposite> instanceReader() {
        return InternalComposite::new;
    }

    @Override
    protected Class<ParsedComposite> implementationClass() {
        return ParsedComposite.class;
    }

    protected <P extends ParsedAggregation> P parseAndAssert(final InternalAggregation aggregation,
                                                             final boolean shuffled, final boolean addRandomFields) throws IOException {
        return super.parseAndAssert(aggregation, false, false);
    }

    private CompositeKey createCompositeKey() {
        Comparable<?>[] keys = new Comparable<?>[sourceNames.size()];
        for (int j = 0; j  < keys.length; j++) {
            switch (formats[j]) {
                case 0:
                    keys[j] = randomLong();
                    break;
                case 1:
                    keys[j] = randomDouble();
                    break;
                case 2:
                    keys[j] = new BytesRef(randomAsciiLettersOfLengthBetween(1, 20));
                    break;
                default:
                    throw new AssertionError("illegal branch");
            }
        }
        return new CompositeKey(keys);
    }

    @SuppressWarnings("unchecked")
    private Comparator<CompositeKey> getKeyComparator() {
        return (o1, o2) -> {
            for (int i = 0; i < o1.size(); i++) {
                int cmp = ((Comparable) o1.get(i)).compareTo(o2.get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    @SuppressWarnings("unchecked")
    private Comparator<InternalComposite.InternalBucket> getBucketComparator() {
        return (o1, o2) -> {
            for (int i = 0; i < o1.getRawKey().size(); i++) {
                int cmp = ((Comparable) o1.getRawKey().get(i)).compareTo(o2.getRawKey().get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    @Override
    protected InternalComposite createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData, InternalAggregations aggregations) {
        int numBuckets = randomIntBetween(0, size);
        List<InternalComposite.InternalBucket> buckets = new ArrayList<>();
        TreeSet<CompositeKey> keys = new TreeSet<>(getKeyComparator());
        for (int i = 0;  i < numBuckets; i++) {
            final CompositeKey key = createCompositeKey();
            if (keys.contains(key)) {
                continue;
            }
            keys.add(key);
            InternalComposite.InternalBucket bucket =
                new InternalComposite.InternalBucket(sourceNames, key, reverseMuls, 1L, aggregations);
            buckets.add(bucket);
        }
        Collections.sort(buckets, (o1, o2) -> o1.compareKey(o2));
        return new InternalComposite(name, size, sourceNames, buckets, reverseMuls, Collections.emptyList(), metaData);
    }

    @Override
    protected InternalComposite mutateInstance(InternalComposite instance) throws IOException {
        List<InternalComposite.InternalBucket> buckets = instance.getBuckets();
        Map<String, Object> metaData = instance.getMetaData();
        int code = randomIntBetween(0, 2);
        int[] reverseMuls = instance.getReverseMuls();
        switch(code) {
            case 0:
                int[] newReverseMuls = new int[reverseMuls.length];
                for (int i = 0; i < reverseMuls.length; i++) {
                    newReverseMuls[i] = reverseMuls[i] == 1 ? -1 : 1;
                }
                reverseMuls = newReverseMuls;
                break;
            case 1:
                buckets = new ArrayList<>(buckets);
                buckets.add(new InternalComposite.InternalBucket(sourceNames, createCompositeKey(), reverseMuls,
                    randomLongBetween(1, 100), InternalAggregations.EMPTY)
                );
                break;
            case 2:
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("illegal branch");
        }
        return new InternalComposite(instance.getName(), instance.getSize(), sourceNames, buckets, reverseMuls,
            instance.pipelineAggregators(), metaData);
    }

    @Override
    protected void assertReduced(InternalComposite reduced, List<InternalComposite> inputs) {
        List<CompositeKey> expectedKeys = inputs.stream()
            .flatMap((s) -> s.getBuckets().stream())
            .map(InternalComposite.InternalBucket::getRawKey)
            .sorted(getKeyComparator())
            .distinct()
            .limit(reduced.getSize())
            .collect(Collectors.toList());

        assertThat(reduced.getBuckets().size(), lessThanOrEqualTo(size));
        assertThat(reduced.getBuckets().size(), equalTo(expectedKeys.size()));
        Iterator<CompositeKey> expectedIt = expectedKeys.iterator();
        for (InternalComposite.InternalBucket bucket : reduced.getBuckets()) {
            assertTrue(expectedIt.hasNext());
            assertThat(bucket.getRawKey(), equalTo(expectedIt.next()));
        }
        assertFalse(expectedIt.hasNext());
    }

    public void testReduceSame() throws IOException {
        InternalComposite result = createTestInstance(randomAlphaOfLength(10), Collections.emptyList(), Collections.emptyMap(),
            InternalAggregations.EMPTY);
        List<InternalAggregation> toReduce = new ArrayList<>();
        int numSame = randomIntBetween(1, 10);
        for (int i = 0; i < numSame; i++) {
            toReduce.add(result);
        }
        InternalComposite finalReduce = (InternalComposite) result.reduce(toReduce,
            new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, true));
        assertThat(finalReduce.getBuckets().size(), equalTo(result.getBuckets().size()));
        Iterator<InternalComposite.InternalBucket> expectedIt = result.getBuckets().iterator();
        for (InternalComposite.InternalBucket bucket : finalReduce.getBuckets()) {
            InternalComposite.InternalBucket expectedBucket = expectedIt.next();
            assertThat(bucket.getRawKey(), equalTo(expectedBucket.getRawKey()));
            assertThat(bucket.getDocCount(), equalTo(expectedBucket.getDocCount()*numSame));
        }
    }
}
