/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper.fielddata;


import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;

/**
 * Per-document Hll value. represented as a RunLen iterator.
 */
public abstract class HllValue implements AbstractHyperLogLog.RunLenIterator {

    /**
     * Skips over and discards n bytes of data from this HLL value.
     * @param registers the number of registers to skip
     */
    protected abstract void skip(int registers);

    /**
     * Advance and merge the next {@code registersToMerge} values and return the resulting value.
     *
     * @param precisionDiff  The precision difference related to the merge
     * @param registersToMerge number of register to merge. This value should be equal to 1 &lt;&lt; precisionDiff
     * @return the merged value
     */
    public byte mergeRegisters(int precisionDiff, int registersToMerge) {
        assert (1 << precisionDiff) == registersToMerge;
        for (int i = 0; i < registersToMerge; i++) {
            next();
            final byte runLen = value();
            if (runLen != 0) {
                skip(registersToMerge - i - 1);
                if (i == 0) {
                    // If the first element is set, then runLen is the current runLen plus the change in precision
                    return (byte) (runLen + precisionDiff);
                } else {
                    // If any other register is set, the runLen is computed from the register position
                    return (byte) (precisionDiff - (int) (Math.log(i) / Math.log(2)));
                }
            }
        }
        // No value for this register
        return 0;
    }
}
