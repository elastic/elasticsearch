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
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SignificantStringTermsTests extends InternalSignificantTermsTestCase {

    private SignificanceHeuristic significanceHeuristic;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        significanceHeuristic = randomSignificanceHeuristic();
    }

    @Override
    protected InternalSignificantTerms createTestInstance(String name,
                                                          List<PipelineAggregator> pipelineAggregators,
                                                          Map<String, Object> metaData,
                                                          InternalAggregations aggregations) {
        DocValueFormat format = DocValueFormat.RAW;
        int requiredSize = randomIntBetween(1, 5);
        int shardSize = requiredSize + 2;
        final int numBuckets = randomInt(shardSize);

        long globalSubsetSize = 0;
        long globalSupersetSize = 0;

        List<SignificantStringTerms.Bucket> buckets = new ArrayList<>(numBuckets);
        Set<BytesRef> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));

            int subsetDf = randomIntBetween(1, 10);
            int supersetDf = randomIntBetween(subsetDf, 20);
            int supersetSize = randomIntBetween(supersetDf, 30);

            globalSubsetSize += subsetDf;
            globalSupersetSize += supersetSize;

            buckets.add(new SignificantStringTerms.Bucket(term, subsetDf, subsetDf, supersetDf, supersetSize, aggregations, format));
        }
        return new SignificantStringTerms(name, requiredSize, 1L, pipelineAggregators, metaData, format, globalSubsetSize,
                globalSupersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Writeable.Reader<InternalSignificantTerms<?, ?>> instanceReader() {
        return SignificantStringTerms::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedSignificantStringTerms.class;
    }

    private static SignificanceHeuristic randomSignificanceHeuristic() {
        return randomFrom(
                new JLHScore(),
                new MutualInformation(randomBoolean(), randomBoolean()),
                new GND(randomBoolean()),
                new ChiSquare(randomBoolean(), randomBoolean()));
    }
}
