/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.document.FeatureField;

/**
 * This class is forked from the Lucene {@link FeatureField} implementation to enable support for storing term vectors.
 * Its purpose is to allow decoding the feature value from the term frequency
 */
public final class XFeatureField {
    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

    static float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            // This is never used in practice but callers of the SimScorer API might
            // occasionally call it on eg. Float.MAX_VALUE to compute the max score
            // so we need to be consistent.
            return Float.MAX_VALUE;
        }
        int tf = (int) freq; // lossless
        int featureBits = tf << 15;
        return Float.intBitsToFloat(featureBits);
    }
}
