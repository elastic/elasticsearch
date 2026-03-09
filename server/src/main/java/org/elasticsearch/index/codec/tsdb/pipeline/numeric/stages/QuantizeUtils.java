/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;

final class QuantizeUtils {

    private QuantizeUtils() {}

    // NOTE: Kahan bias rounding constants — 2^52+2^51 for double, 2^23+2^22 for float.
    private static final double ROUNDING_BIAS_DOUBLE = 6755399441055744.0;
    private static final double FAST_ROUND_MAX_DOUBLE = (double) (1L << 52);
    private static final float ROUNDING_BIAS_FLOAT = 12582912.0f;
    private static final float FAST_ROUND_MAX_FLOAT = (float) (1 << 23);

    static long roundDouble(double x) {
        if (x > -FAST_ROUND_MAX_DOUBLE && x < FAST_ROUND_MAX_DOUBLE) {
            if (x >= 0) {
                return (long) (x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE;
            }
            return -((long) (-x + ROUNDING_BIAS_DOUBLE) - (long) ROUNDING_BIAS_DOUBLE);
        }
        return Math.round(x);
    }

    static int roundFloat(float x) {
        if (x > -FAST_ROUND_MAX_FLOAT && x < FAST_ROUND_MAX_FLOAT) {
            if (x >= 0) {
                return (int) (x + ROUNDING_BIAS_FLOAT) - (int) ROUNDING_BIAS_FLOAT;
            }
            return -((int) (-x + ROUNDING_BIAS_FLOAT) - (int) ROUNDING_BIAS_FLOAT);
        }
        return Math.round(x);
    }

    static void quantizeDoubles(final long[] values, int valueCount, double step) {
        for (int i = 0; i < valueCount; i++) {
            final double v = NumericUtils.sortableLongToDouble(values[i]);
            if (Double.isFinite(v)) {
                values[i] = NumericUtils.doubleToSortableLong(roundDouble(v / step) * step);
            }
        }
    }

    static void quantizeFloats(final long[] values, int valueCount, float step) {
        for (int i = 0; i < valueCount; i++) {
            final float v = NumericUtils.sortableIntToFloat((int) values[i]);
            if (Float.isFinite(v)) {
                values[i] = NumericUtils.floatToSortableInt(roundFloat(v / step) * step);
            }
        }
    }
}
