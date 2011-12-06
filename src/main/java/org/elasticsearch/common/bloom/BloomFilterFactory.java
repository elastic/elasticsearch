/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bloom;

import org.elasticsearch.common.UUID;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.UnsupportedEncodingException;

/**
 *
 */
public class BloomFilterFactory {

    private static ESLogger logger = ESLoggerFactory.getLogger(BloomFilterFactory.class.getName());

    private static final int EXCESS = 20;

    /**
     * @return A BloomFilter with the lowest practical false positive probability
     *         for the given number of elements.
     */
    public static BloomFilter getFilter(long numElements, int targetBucketsPerElem) {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem) {
            logger.warn(String.format("Cannot provide an optimal BloomFilter for %d elements (%d/%d buckets per element).",
                    numElements, bucketsPerElement, targetBucketsPerElem));
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return new ObsBloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    /**
     * @return The smallest BloomFilter that can provide the given false positive
     *         probability rate for the given number of elements.
     *         <p/>
     *         Asserts that the given probability can be satisfied using this filter.
     */
    public static BloomFilter getFilter(long numElements, double maxFalsePosProbability) {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return new ObsBloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    private static long bucketsFor(long numElements, int bucketsPer) {
        return numElements * bucketsPer + EXCESS;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        long elements = SizeValue.parseSizeValue("100m").singles();
        BloomFilter filter = BloomFilterFactory.getFilter(elements, 15);
        System.out.println("Filter size: " + new ByteSizeValue(filter.sizeInBytes()));
        for (long i = 0; i < elements; i++) {
            byte[] utf8s = UUID.randomBase64UUID().getBytes("UTF8");
            filter.add(utf8s, 0, utf8s.length);
        }
        long falsePositives = 0;
        for (long i = 0; i < elements; i++) {
            byte[] utf8s = UUID.randomBase64UUID().getBytes("UTF8");
            if (filter.isPresent(utf8s, 0, utf8s.length)) {
                falsePositives++;
            }
        }
        System.out.println("false positives: " + falsePositives);

        byte[] utf8s = UUID.randomBase64UUID().getBytes("UTF8");
        long time = System.currentTimeMillis();
        for (long i = 0; i < elements; i++) {
            if (filter.isPresent(utf8s, 0, utf8s.length)) {
            }
        }
        long timeSize = System.currentTimeMillis() - time;
        System.out.println("Indexed in " + new TimeValue(timeSize) + ", TPS: " + (elements / timeSize) + " per millisecond");
    }
}