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

import org.apache.lucene.util.BytesRef;
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

public class SignificantStringTermsTests extends InternalSignificantTermsTestCase {

    @Override
    protected InternalSignificantTerms createTestInstance(String name,
                                                          List<PipelineAggregator> pipelineAggregators,
                                                          Map<String, Object> metaData,
                                                          InternalAggregations aggs,
                                                          int requiredSize, int numBuckets,
                                                          long subsetSize, int[] subsetDfs,
                                                          long supersetSize, int[] supersetDfs,
                                                          SignificanceHeuristic significanceHeuristic) {
        DocValueFormat format = DocValueFormat.RAW;
        List<SignificantStringTerms.Bucket> buckets = new ArrayList<>(numBuckets);
        Set<BytesRef> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));
            SignificantStringTerms.Bucket bucket = new SignificantStringTerms.Bucket(term, subsetDfs[i], subsetSize, 
                    supersetDfs[i], supersetSize, aggs, format, 0);
            bucket.updateScore(significanceHeuristic);
            buckets.add(bucket);
        }
        return new SignificantStringTerms(name, requiredSize, 1L, pipelineAggregators, metaData, format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Writeable.Reader<InternalSignificantTerms<?, ?>> instanceReader() {
        return SignificantStringTerms::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedSignificantStringTerms.class;
    }

    @Override
    protected InternalSignificantTerms<?, ?> mutateInstance(InternalSignificantTerms<?, ?> instance) {
        if (instance instanceof SignificantStringTerms) {
            SignificantStringTerms stringTerms = (SignificantStringTerms) instance;
            String name = stringTerms.getName();
            int requiredSize = stringTerms.requiredSize;
            long minDocCount = stringTerms.minDocCount;
            DocValueFormat format = stringTerms.format;
            long subsetSize = stringTerms.getSubsetSize();
            long supersetSize = stringTerms.getSupersetSize();
            List<SignificantStringTerms.Bucket> buckets = stringTerms.getBuckets();
            SignificanceHeuristic significanceHeuristic = stringTerms.significanceHeuristic;
            List<PipelineAggregator> pipelineAggregators = stringTerms.pipelineAggregators();
            Map<String, Object> metaData = stringTerms.getMetaData();
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
                buckets.add(new SignificantStringTerms.Bucket(new BytesRef(randomAlphaOfLengthBetween(1, 10)),
                        randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                        InternalAggregations.EMPTY, format, 0));
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
            return new SignificantStringTerms(name, requiredSize, minDocCount, pipelineAggregators, metaData, format, subsetSize,
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
