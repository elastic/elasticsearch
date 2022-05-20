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
 * Default policy is to allocate a bitset with 10% saturation given a unique term per document. Bits
 * are set via MurmurHash2 hashing function.
 *
 * @lucene.experimental
 */
public class DefaultBloomFilterFactory extends BloomFilterFactory {

    @Override
    public FuzzySet getSetForField(SegmentWriteState state, FieldInfo info) {
        // Assume all of the docs have a unique term (e.g. a primary key) and we hope to maintain a set
        // with 10% of bits set
        return FuzzySet.createSetBasedOnQuality(state.segmentInfo.maxDoc(), 0.10f);
    }

    @Override
    public boolean isSaturated(FuzzySet bloomFilter, FieldInfo fieldInfo) {
        // Don't bother saving bitsets if >90% of bits are set - we don't want to
        // throw any more memory at this problem.
        return bloomFilter.getSaturation() > 0.9f;
    }
}
