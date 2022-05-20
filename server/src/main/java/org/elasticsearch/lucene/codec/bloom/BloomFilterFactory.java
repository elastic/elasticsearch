/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
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
package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Class used to create index-time {@link FuzzySet} appropriately configured for each field. Also
 * called to right-size bitsets for serialization.
 *
 * @lucene.experimental
 */
public abstract class BloomFilterFactory {

    /**
     * @param state The content to be indexed
     * @param info the field requiring a BloomFilter
     * @return An appropriately sized set or null if no BloomFiltering required
     */
    public abstract FuzzySet getSetForField(SegmentWriteState state, FieldInfo info);

    /**
     * Called when downsizing bitsets for serialization
     *
     * @param fieldInfo The field with sparse set bits
     * @param initialSet The bits accumulated
     * @return null or a hopefully more densely packed, smaller bitset
     */
    public FuzzySet downsize(FieldInfo fieldInfo, FuzzySet initialSet) {
        // Aim for a bitset size that would have 10% of bits set (so 90% of searches
        // would fail-fast)
        float targetMaxSaturation = 0.1f;
        return initialSet.downsize(targetMaxSaturation);
    }

    /**
     * Used to determine if the given filter has reached saturation and should be retired i.e. not
     * saved any more
     *
     * @param bloomFilter The bloomFilter being tested
     * @param fieldInfo The field with which this filter is associated
     * @return true if the set has reached saturation and should be retired
     */
    public abstract boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo);
}
