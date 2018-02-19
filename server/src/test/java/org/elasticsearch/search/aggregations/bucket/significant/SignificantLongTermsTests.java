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

package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SignificantLongTermsTests extends InternalSignificantTermsTestCase {

    private DocValueFormat format;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();
    }

    @Override
    protected InternalSignificantTerms createTestInstance(String name,
                                                          List<PipelineAggregator> pipelineAggregators,
                                                          Map<String, Object> metaData,
                                                          InternalAggregations aggs,
                                                          int requiredSize, int numBuckets,
                                                          long subsetSize, int[] subsetDfs,
                                                          long supersetSize, int[] supersetDfs,
                                                          SignificanceHeuristic significanceHeuristic) {

        List<SignificantLongTerms.Bucket> buckets = new ArrayList<>(numBuckets);
        Set<Long> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            long term = randomValueOtherThanMany(l -> terms.add(l) == false, random()::nextLong);
            buckets.add(new SignificantLongTerms.Bucket(subsetDfs[i], subsetSize, supersetDfs[i], supersetSize, term, aggs, format));
        }
        return new SignificantLongTerms(name, requiredSize, 1L, pipelineAggregators, metaData, format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Writeable.Reader<InternalSignificantTerms<?, ?>> instanceReader() {
        return SignificantLongTerms::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedSignificantLongTerms.class;
    }

    @Override
    protected InternalSignificantTerms<?, ?> mutateInstance(InternalSignificantTerms<?, ?> instance) {
        if (instance instanceof SignificantLongTerms) {
            SignificantLongTerms longTerms = (SignificantLongTerms) instance;
            String name = longTerms.getName();
            int requiredSize = longTerms.requiredSize;
            long minDocCount = longTerms.minDocCount;
            DocValueFormat format = longTerms.format;
            long subsetSize = longTerms.getSubsetSize();
            long supersetSize = longTerms.getSupersetSize();
            List<SignificantLongTerms.Bucket> buckets = longTerms.getBuckets();
            SignificanceHeuristic significanceHeuristic = longTerms.significanceHeuristic;
            List<PipelineAggregator> pipelineAggregators = longTerms.pipelineAggregators();
            Map<String, Object> metaData = longTerms.getMetaData();
            switch (between(0, 5)) {
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
                subsetSize += between(1, 100);
                break;
            case 4:
                supersetSize += between(1, 100);
                break;
            case 5:
                buckets = new ArrayList<>(buckets);
                buckets.add(new SignificantLongTerms.Bucket(randomLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                        randomNonNegativeLong(), randomNonNegativeLong(), InternalAggregations.EMPTY, format));
                break;
            case 8:
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return new SignificantLongTerms(name, requiredSize, minDocCount, pipelineAggregators, metaData, format, subsetSize,
                    supersetSize, significanceHeuristic, buckets);
        } else {
            String name = instance.getName();
            int requiredSize = instance.requiredSize;
            long minDocCount = instance.minDocCount;
            List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
            Map<String, Object> metaData = instance.getMetaData();
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
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return new UnmappedSignificantTerms(name, requiredSize, minDocCount, pipelineAggregators, metaData);
        }
    }
}
