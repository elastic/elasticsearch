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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringTermsTests extends InternalTermsTestCase {
    @Override
    protected InternalTerms<?, ?> createTestInstance(String name,
                                                     Map<String, Object> metadata,
                                                     InternalAggregations aggregations,
                                                     boolean showTermDocCountError,
                                                     long docCountError) {
        return createTestInstance(generateRandomDict(), name, metadata, aggregations, showTermDocCountError, docCountError);
    }

    @Override
    protected List<InternalTerms<?, ?>> randomResultsToReduce(String name, int size) {
        List<InternalTerms<?, ?>> inputs = new ArrayList<>();
        BytesRef[] dict = generateRandomDict();
        for (int i = 0; i < size; i++) {
            InternalTerms<?, ?> t = randomBoolean() ? createUnmappedInstance(name) : createTestInstance(dict, name);
            inputs.add(t);
        }
        return inputs;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedStringTerms.class;
    }

    @Override
    protected InternalTerms<?, ?> mutateInstance(InternalTerms<?, ?> instance) {
        if (instance instanceof StringTerms) {
            StringTerms stringTerms = (StringTerms) instance;
            String name = stringTerms.getName();
            BucketOrder order = stringTerms.order;
            int requiredSize = stringTerms.requiredSize;
            long minDocCount = stringTerms.minDocCount;
            DocValueFormat format = stringTerms.format;
            int shardSize = stringTerms.getShardSize();
            boolean showTermDocCountError = stringTerms.showTermDocCountError;
            long otherDocCount = stringTerms.getSumOfOtherDocCounts();
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            long docCountError = stringTerms.getDocCountError();
            Map<String, Object> metadata = stringTerms.getMetadata();
            switch (between(0, 8)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    requiredSize += between(1, 100);
                    break;
                case 2:
                    minDocCount += between(1, 100);
                    break;
                case 3:
                    shardSize += between(1, 100);
                    break;
                case 4:
                    showTermDocCountError = showTermDocCountError == false;
                    break;
                case 5:
                    otherDocCount += between(1, 100);
                    break;
                case 6:
                    docCountError += between(1, 100);
                    break;
                case 7:
                    buckets = new ArrayList<>(buckets);
                    buckets.add(new StringTerms.Bucket(new BytesRef(randomAlphaOfLengthBetween(1, 10)), randomNonNegativeLong(),
                        InternalAggregations.EMPTY, showTermDocCountError, docCountError, format));
                    break;
                case 8:
                    if (metadata == null) {
                        metadata = new HashMap<>(1);
                    } else {
                        metadata = new HashMap<>(instance.getMetadata());
                    }
                    metadata.put(randomAlphaOfLength(15), randomInt());
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            Collections.sort(buckets, stringTerms.reduceOrder.comparator());
            return new StringTerms(name, stringTerms.reduceOrder, order, requiredSize, minDocCount, metadata, format, shardSize,
                showTermDocCountError, otherDocCount, buckets, docCountError);
        } else {
            String name = instance.getName();
            BucketOrder order = instance.order;
            int requiredSize = instance.requiredSize;
            long minDocCount = instance.minDocCount;
            Map<String, Object> metadata = instance.getMetadata();
            switch (between(0, 3)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    requiredSize += between(1, 100);
                    break;
                case 2:
                    minDocCount += between(1, 100);
                    break;
                case 3:
                    if (metadata == null) {
                        metadata = new HashMap<>(1);
                    } else {
                        metadata = new HashMap<>(instance.getMetadata());
                    }
                    metadata.put(randomAlphaOfLength(15), randomInt());
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            return new UnmappedTerms(name, order, requiredSize, minDocCount, metadata);
        }
    }

    private BytesRef[] generateRandomDict() {
        Set<BytesRef> terms = new HashSet<>();
        int numTerms = randomIntBetween(2, 100);
        for (int i = 0; i < numTerms; i++) {
            terms.add(new BytesRef(randomAlphaOfLength(10)));
        }
        return terms.stream().toArray(BytesRef[]::new);
    }

    private InternalTerms<?, ?> createTestInstance(BytesRef[] dict, String name) {
        return createTestInstance(dict, name, createTestMetadata(), createSubAggregations(), showDocCount, docCountError);
    }

    private InternalTerms<?, ?> createTestInstance(BytesRef[] dict,
                                                   String name,
                                                   Map<String, Object> metadata,
                                                   InternalAggregations aggregations,
                                                   boolean showTermDocCountError,
                                                   long docCountError) {
        BucketOrder order = BucketOrder.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize + 2;
        DocValueFormat format = DocValueFormat.RAW;
        long otherDocCount = 0;
        List<StringTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        Set<BytesRef> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BytesRef term = dict[randomIntBetween(0, dict.length-1)];
            if (terms.add(term)) {
                int docCount = randomIntBetween(1, 100);
                buckets.add(new StringTerms.Bucket(term, docCount, aggregations, showTermDocCountError, docCountError, format));
            }
        }
        BucketOrder reduceOrder = randomBoolean() ?
            BucketOrder.compound(BucketOrder.key(true), BucketOrder.count(false)) : BucketOrder.key(true);
        Collections.sort(buckets, reduceOrder.comparator());
        return new StringTerms(name, reduceOrder, order, requiredSize, minDocCount,
                metadata, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }
}
