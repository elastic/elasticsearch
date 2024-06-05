/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

final class SizeEstimatorHelper {

    private SizeEstimatorHelper() {}

    private static final int STRING_SIZE = (int) shallowSizeOfInstance(String.class);

    static long sizeOfString(int stringLength) {
        // Technically, each value counted in a String.length is 2 bytes. But, this is how `RamUsageEstimator` calculates it
        return alignObjectSize(STRING_SIZE + (long) NUM_BYTES_ARRAY_HEADER + (long) (Character.BYTES) * stringLength);
    }

    static long sizeOfStringCollection(int[] stringSizes) {
        long shallow = alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * stringSizes.length);
        return shallow + Arrays.stream(stringSizes).mapToLong(SizeEstimatorHelper::sizeOfString).sum();
    }

    static long sizeOfDoubleArray(int arrayLength) {
        return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * arrayLength);
    }

    static long sizeOfHashMap(List<Long> sizeOfKeys, List<Long> sizeOfValues) {
        assert sizeOfKeys.size() == sizeOfValues.size();
        long mapsize = shallowSizeOfInstance(HashMap.class);
        final long mapEntrySize = shallowSizeOfInstance(HashMap.Entry.class);
        mapsize += Stream.concat(sizeOfKeys.stream(), sizeOfValues.stream()).mapToLong(Long::longValue).sum();
        mapsize += mapEntrySize * sizeOfKeys.size();
        mapsize += mapEntrySize * sizeOfValues.size();
        return mapsize;
    }
}
