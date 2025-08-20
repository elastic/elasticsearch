/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

public class ExponentialHistogramUtils {

    /**
     * Estimates the sum of all values of a histogram just based on the populated buckets.
     *
     * @param negativeBuckets the negative buckets of the histogram
     * @param positiveBuckets the positive buckets of the histogram
     * @return the estimated sum of all values in the histogram, guaranteed to be zero if there are no buckets
     */
    public static double estimateSum(BucketIterator negativeBuckets, BucketIterator positiveBuckets) {
        double sum = 0.0;
        while (negativeBuckets.hasNext()) {
            double bucketMidPoint = ExponentialScaleUtils.getPointOfLeastRelativeError(
                negativeBuckets.peekIndex(),
                negativeBuckets.scale()
            );
            sum += -bucketMidPoint * negativeBuckets.peekCount();
            negativeBuckets.advance();
        }
        while (positiveBuckets.hasNext()) {
            double bucketMidPoint = ExponentialScaleUtils.getPointOfLeastRelativeError(
                positiveBuckets.peekIndex(),
                positiveBuckets.scale()
            );
            sum += bucketMidPoint * positiveBuckets.peekCount();
            positiveBuckets.advance();
        }
        return sum;
    }
}
