/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.simdvec.AshSphericalScalarQuantizer;

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.D2F;
import static jdk.incubator.vector.VectorOperators.D2I;
import static jdk.incubator.vector.VectorOperators.F2D;
import static jdk.incubator.vector.VectorOperators.GE;
import static jdk.incubator.vector.VectorOperators.GT;
import static jdk.incubator.vector.VectorOperators.I2D;
import static jdk.incubator.vector.VectorOperators.LE;
import static jdk.incubator.vector.VectorOperators.LT;
import static org.elasticsearch.simdvec.internal.vectorization.PanamaESVectorUtilSupport.fma;

public final class PanamaAshSphericalScalarQuantizer extends AshSphericalScalarQuantizer {

    private static final VectorSpecies<Float> FLOAT_SPECIES = PanamaVectorConstants.PREFERRED_FLOAT_SPECIES;
    private static final VectorSpecies<Integer> INTEGER_SPECIES = PanamaVectorConstants.PREFERRED_INTEGER_SPECIES;

    public PanamaAshSphericalScalarQuantizer(int bitsPerDim) {
        super(bitsPerDim);
    }

    @Override
    protected float quantizeExact1Bit(float[] z, int zOffset, float[] out, int outOffset, int d) {
        // on smaller vector sizes, the JVM is better at auto-vectorizing the scalar impl
        // On AVX512, the vector code + mask gets us significant speedups
        if (PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE <= 256) {
            return super.quantizeExact1Bit(z, zOffset, out, outOffset, d);
        }
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
    protected float calculateBaseLevel(float[] z, int zOffset, int[] absZF) {
        final int limit = FLOAT_SPECIES.loopBound(absZF.length);
        FloatVector halfConst = FloatVector.broadcast(FLOAT_SPECIES, 0.5f);

        FloatVector dotAcc = FloatVector.zero(FLOAT_SPECIES);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector abs = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i).abs();
            abs.reinterpretAsInts().intoArray(absZF, i);
            dotAcc = fma(halfConst, abs, dotAcc);
        }
        if (i < absZF.length) {
            var mask = FLOAT_SPECIES.indexInRange(i, absZF.length);
            FloatVector abs = FloatVector.fromArray(FLOAT_SPECIES, z, zOffset + i, mask).abs();
            abs.reinterpretAsInts().intoArray(absZF, i, mask.cast(INTEGER_SPECIES));
            dotAcc = fma(halfConst, abs, dotAcc);
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

    private static final VectorSpecies<Double> DOUBLE_SPECIES = PanamaVectorConstants.PREFERRED_DOUBLE_SPECIES;
    private static final VectorSpecies<Float> HALF_FLOAT_SPECIES;
    private static final VectorSpecies<Integer> HALF_INTEGER_SPECIES;

    static {
        VectorSpecies<Float> halfFloat;
        VectorSpecies<Integer> halfInteger;
        try {
            VectorShape halfDoubleBits = VectorShape.forBitSize(DOUBLE_SPECIES.vectorBitSize() / 2);
            halfFloat = VectorSpecies.of(float.class, halfDoubleBits);
            halfInteger = VectorSpecies.of(int.class, halfDoubleBits);
        } catch (IllegalArgumentException e) {
            halfFloat = null;
            halfInteger = null;
        }
        HALF_FLOAT_SPECIES = halfFloat;
        HALF_INTEGER_SPECIES = halfInteger;
    }

    @Override
    protected void setGeneralOutput(float[] z, int zOffset, float[] out, int outOffset, int d, int nSteps, int bestStep, double bestMag) {
        if (HALF_FLOAT_SPECIES == null) {
            // uh oh, can't get half vector sizes for some reason, fallback
            super.setGeneralOutput(z, zOffset, out, outOffset, d, nSteps, bestStep, bestMag);
            return;
        }

        /*
         * Calcs need to be done at double precision, so pull a half-vector of floats
         * each iteration and expand up to doubles
         */
        final int limit = HALF_FLOAT_SPECIES.loopBound(d);
        FloatVector halfConst = FloatVector.broadcast(HALF_FLOAT_SPECIES, 0.5f);
        IntVector signBit = IntVector.broadcast(HALF_INTEGER_SPECIES, 0x80000000);
        DoubleVector nStepsVec = DoubleVector.broadcast(DOUBLE_SPECIES, (double) nSteps);
        DoubleVector bestStepVec = DoubleVector.broadcast(DOUBLE_SPECIES, (double) bestStep);
        DoubleVector bestMagVec = DoubleVector.broadcast(DOUBLE_SPECIES, bestMag);

        int i = 0;
        for (; i < limit; i += HALF_FLOAT_SPECIES.length()) {
            FloatVector vec = FloatVector.fromArray(HALF_FLOAT_SPECIES, z, zOffset + i);
            // double scaled = bestStep * (double) Math.abs(v);
            Vector<Double> scaled = vec.abs().convertShape(F2D, DOUBLE_SPECIES, 0).mul(bestStepVec);
            // int levels = (int) Math.min(scaled / bestMag, nSteps);
            // truncate to integer and convert back
            DoubleVector levels = (DoubleVector) scaled.div(bestMagVec).min(nStepsVec).convert(D2I, 0).convert(I2D, 0);

            // level correction checks (do it all in doubles for simplicity)
            var posMask = levels.compare(LT, nSteps).and(levels.add(1).mul(bestMagVec).compare(LE, scaled));
            var negMask = levels.compare(GT, 0).and(levels.mul(bestMagVec).compare(GT, scaled));
            levels = levels.add(1, posMask);
            levels = levels.add(-1, negMask.andNot(posMask));

            // out[outOffset + j] = Math.copySign(0.5f + levels, v);
            IntVector result = levels.convertShape(D2F, HALF_FLOAT_SPECIES, 0)
                .add(halfConst)
                .reinterpretAsInts()
                .or(vec.reinterpretAsInts().and(signBit));
            result.reinterpretAsFloats().intoArray(out, outOffset + i);
        }
        if (i < d) {
            var mask = HALF_FLOAT_SPECIES.indexInRange(i, d);
            FloatVector vec = FloatVector.fromArray(HALF_FLOAT_SPECIES, z, zOffset + i, mask);
            Vector<Double> scaled = vec.abs().convertShape(F2D, DOUBLE_SPECIES, 0).mul(bestStepVec);
            DoubleVector levels = (DoubleVector) scaled.div(bestMagVec).min(nStepsVec).convert(D2I, 0).convert(I2D, 0);

            var posMask = levels.compare(LT, nSteps).and(levels.add(1).mul(bestMagVec).compare(LE, scaled));
            var negMask = levels.compare(GT, 0).and(levels.mul(bestMagVec).compare(GT, scaled));
            levels = levels.add(1, posMask);
            levels = levels.add(-1, negMask.andNot(posMask));

            IntVector result = levels.convertShape(D2F, HALF_FLOAT_SPECIES, 0)
                .add(halfConst)
                .reinterpretAsInts()
                .or(vec.reinterpretAsInts().and(signBit));
            result.reinterpretAsFloats().intoArray(out, outOffset + i, mask);
        }
    }
}
