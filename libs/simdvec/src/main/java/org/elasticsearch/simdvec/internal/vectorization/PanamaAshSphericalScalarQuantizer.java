/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.simdvec.AshSphericalScalarQuantizer;

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.GE;

public final class PanamaAshSphericalScalarQuantizer extends AshSphericalScalarQuantizer {

    private static final VectorSpecies<Float> FLOAT_SPECIES = PanamaVectorConstants.PREFERRED_FLOAT_SPECIES;
    private static final VectorSpecies<Integer> INTEGER_SPECIES = PanamaVectorConstants.PREFERRED_INTEGER_SPECIES;

    public PanamaAshSphericalScalarQuantizer(int bitsPerDim) {
        super(bitsPerDim);
    }

    @Override
    protected float quantizeExact1Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        IntVector halfConst = FloatVector.broadcast(FLOAT_SPECIES, 0.5f).reinterpretAsInts();
        IntVector signBit = IntVector.broadcast(INTEGER_SPECIES, 0x80000000);

        int i = 0;
        int limit = FLOAT_SPECIES.loopBound(d);
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            IntVector vec = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i).reinterpretAsInts();
            // Use the sign bit from vec, but the rest of the value from halfConst
            IntVector result = halfConst.or(vec.and(signBit));
            result.reinterpretAsFloats().intoArray(out, outOffset + i);
        }
        if (i < d) {
            var mask = FLOAT_SPECIES.indexInRange(i, d);
            IntVector vec = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i, mask).reinterpretAsInts();
            IntVector result = halfConst.or(vec.and(signBit));  // don't need to mask this
            result.reinterpretAsFloats().intoArray(out, outOffset + i, mask);
        }

        return (float) Math.sqrt(0.25 * d);
    }

    @Override
    protected double calculateBaseLevel(float[] z, int zOffset, int[] absZF) {
        final int limit = FLOAT_SPECIES.loopBound(absZF.length);
        FloatVector halfConst = FloatVector.broadcast(FLOAT_SPECIES, 0.5f);

        FloatVector dotAcc = FloatVector.zero(FLOAT_SPECIES);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector abs = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i).abs();
            abs.reinterpretAsInts().intoArray(absZF, i);
            dotAcc = halfConst.fma(abs, dotAcc);
        }
        if (i < absZF.length) {
            var mask = FLOAT_SPECIES.indexInRange(i, absZF.length);
            FloatVector abs = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i, mask).abs();
            abs.reinterpretAsInts().intoArray(absZF, i, mask.cast(INTEGER_SPECIES));
            dotAcc = halfConst.fma(abs, dotAcc);
        }
        return dotAcc.reduceLanes(ADD);
    }

    @Override
    protected void set2BitOutput(float threshold, float[] z, int zOffset, float[] out, int outOffset, int d) {
        final int limit = FLOAT_SPECIES.loopBound(d);
        FloatVector halfConst = FloatVector.broadcast(FLOAT_SPECIES, 0.5f);
        IntVector oneHalfConst = FloatVector.broadcast(FLOAT_SPECIES, 1.5f).reinterpretAsInts();
        IntVector signBit = IntVector.broadcast(INTEGER_SPECIES, 0x80000000);

        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector vec = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i);
            // Math.copySign(Math.abs(v) >= threshold ? 1.5f : 0.5f, v);
            VectorMask<Integer> nextLevel = vec.abs().compare(GE, threshold).cast(INTEGER_SPECIES);
            IntVector result = halfConst.reinterpretAsInts().blend(oneHalfConst, nextLevel).or(vec.reinterpretAsInts().and(signBit));
            result.reinterpretAsFloats().intoArray(out, outOffset + i);
        }
        if (i < d) {
            var mask = FLOAT_SPECIES.indexInRange(i, d);
            FloatVector vec = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i, mask);
            // Math.copySign(Math.abs(v) >= threshold ? 1.5f : 0.5f, v);
            VectorMask<Integer> nextLevel = vec.abs().compare(GE, threshold).cast(INTEGER_SPECIES);
            IntVector result = halfConst.reinterpretAsInts().blend(oneHalfConst, nextLevel).or(vec.reinterpretAsInts().and(signBit));
            result.reinterpretAsFloats().intoArray(out, outOffset + i, mask);
        }
    }
}
