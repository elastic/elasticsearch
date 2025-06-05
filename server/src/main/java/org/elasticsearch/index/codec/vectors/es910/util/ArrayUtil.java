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
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es910.util;

import static org.apache.lucene.util.ArrayUtil.growExact;
import static org.apache.lucene.util.ArrayUtil.oversize;

public class ArrayUtil {

    public static float[] growInRange(float[] array, int minLength, int maxLength) {
        assert minLength >= 0 : "minLength must be positive (got " + minLength + "): likely integer overflow?";

        if (minLength > maxLength) {
            throw new IllegalArgumentException(
                "requested minimum array length " + minLength + " is larger than requested maximum array length " + maxLength
            );
        }

        if (array.length >= minLength) {
            return array;
        }

        int potentialLength = oversize(minLength, Float.BYTES);
        return growExact(array, Math.min(maxLength, potentialLength));
    }
}
