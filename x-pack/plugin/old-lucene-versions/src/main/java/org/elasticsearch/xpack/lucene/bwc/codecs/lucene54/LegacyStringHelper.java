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
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene54;

import org.apache.lucene.util.BytesRef;

/**
 * Legacy methods for manipulating strings.
 *
 * @deprecated This is only used for backwards compatibility codecs (they
 * don't work with the Java9-based replacement methods).
 */
@Deprecated
abstract class LegacyStringHelper {

    /**
     * Compares two {@link BytesRef}, element by element, and returns the
     * number of elements common to both arrays (from the start of each).
     *
     * @param left The first {@link BytesRef} to compare
     * @param right The second {@link BytesRef} to compare
     * @return The number of common elements (from the start of each).
     */
    public static int bytesDifference(BytesRef left, BytesRef right) {
        int len = left.length < right.length ? left.length : right.length;
        final byte[] bytesLeft = left.bytes;
        final int offLeft = left.offset;
        byte[] bytesRight = right.bytes;
        final int offRight = right.offset;
        for (int i = 0; i < len; i++)
            if (bytesLeft[i + offLeft] != bytesRight[i + offRight]) return i;
        return len;
    }

    /**
     * Returns the length of {@code currentTerm} needed for use as a sort key.
     * so that {@link BytesRef#compareTo(BytesRef)} still returns the same result.
     * This method assumes currentTerm comes after priorTerm.
     */
    public static int sortKeyLength(final BytesRef priorTerm, final BytesRef currentTerm) {
        final int currentTermOffset = currentTerm.offset;
        final int priorTermOffset = priorTerm.offset;
        final int limit = Math.min(priorTerm.length, currentTerm.length);
        for (int i = 0; i < limit; i++) {
            if (priorTerm.bytes[priorTermOffset + i] != currentTerm.bytes[currentTermOffset + i]) {
                return i + 1;
            }
        }
        return Math.min(1 + priorTerm.length, currentTerm.length);
    }

    private LegacyStringHelper() {}

}
