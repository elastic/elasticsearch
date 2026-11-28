/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.expression;

import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ConstantBytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Evaluates a boolean expression that is a pure function of one keyword/text field by running
 * the expression once per distinct referenced dictionary entry of an
 * {@link OrdinalBytesRefBlock} and scattering the results by ordinal.
 * <p>
 * The live dictionary is never wrapped in {@code asBlock()} — {@code BytesRefVectorBlock.closeInternal}
 * would close it. Referenced entries are copied into a new {@link BytesRefVector}.
 * <p>
 * When the field is not dictionary-encoded, is multi-valued, or the referenced-ord density
 * gate fails, evaluation falls through to {@code perRow}. {@code toString} and warnings come
 * from {@code perRow} so the wrapper is invisible to {@code evaluatorToString} tests.
 * <p>
 * Collection is bounded by {@code n}, not dictionary size {@code k}: a Parquet row-group dict
 * with {@code k ≫ n} uses a hash map of at most {@code n/2} entries and aborts when unique
 * referenced ordinals exceed {@code n/2}. After the gate, {@code map} is an array load per row
 * ({@code int[k]} when {@code k ≤ 2n}, else remapped {@code int[n]}). Identity memo keyed by
 * {@code getDictionaryVector() == lastDict} is a follow-up after stable dictionary vectors
 * (esql-planning#2866).
 */
public final class DictionaryPredicateEvaluator implements ExpressionEvaluator {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DictionaryPredicateEvaluator.class);
    static final int MIN_POSITIONS = 10;

    /**
     * Which path the most recent {@link #eval} took. Test-only; not part of {@code toString}.
     */
    public enum Path {
        PER_ROW,
        DICTIONARY,
        CONSTANT_VECTOR
    }

    public record Factory(ExpressionEvaluator.Factory perRow, ExpressionEvaluator.Factory dictionary, int fieldChannel)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            ExpressionEvaluator row = perRow.get(context);
            try {
                return new DictionaryPredicateEvaluator(row, dictionary.get(context), fieldChannel, context);
            } catch (RuntimeException e) {
                Releasables.closeExpectNoException(row);
                throw e;
            }
        }

        @Override
        public String toString() {
            return perRow.toString();
        }

        @Override
        public boolean eagerEvalSafeInLazy() {
            return perRow.eagerEvalSafeInLazy();
        }
    }

    private final ExpressionEvaluator perRow;
    private final ExpressionEvaluator dictionary;
    private final int fieldChannel;
    private final DriverContext driverContext;
    private Boolean nullResult;
    private boolean nullResultComputed;
    Path lastPath;

    DictionaryPredicateEvaluator(
        ExpressionEvaluator perRow,
        ExpressionEvaluator dictionary,
        int fieldChannel,
        DriverContext driverContext
    ) {
        this.perRow = perRow;
        this.dictionary = dictionary;
        this.fieldChannel = fieldChannel;
        this.driverContext = driverContext;
    }

    @Override
    public Block eval(Page page) {
        Block field = page.getBlock(fieldChannel);
        if (field.mayHaveMultivaluedFields() || field instanceof BytesRefBlock == false || field.areAllValuesNull()) {
            lastPath = Path.PER_ROW;
            return perRow.eval(page);
        }
        BytesRefBlock bytes = (BytesRefBlock) field;
        BytesRefVector vector = bytes.asVector();
        if (vector instanceof ConstantBytesRefVector) {
            try {
                Block result = evalConstant(page.getPositionCount(), vector);
                lastPath = Path.CONSTANT_VECTOR;
                return result;
            } catch (IllegalArgumentException e) {
                lastPath = Path.PER_ROW;
                return perRow.eval(page);
            }
        }
        OrdinalBytesRefBlock ordinals = bytes.asOrdinals();
        if (ordinals == null) {
            lastPath = Path.PER_ROW;
            return perRow.eval(page);
        }
        Block result = evalOrdinals(page.getPositionCount(), ordinals);
        if (result != null) {
            lastPath = Path.DICTIONARY;
            return result;
        }
        lastPath = Path.PER_ROW;
        return perRow.eval(page);
    }

    private Block evalConstant(int n, BytesRefVector vector) {
        BlockFactory factory = driverContext.blockFactory();
        BytesRefVector one = null;
        try {
            BytesRef scratch = new BytesRef();
            one = factory.newConstantBytesRefVector(vector.getBytesRef(0, scratch), 1);
            try (Page p = new Page(one.asBlock())) {
                one = null;
                Block raw = dictionary.eval(p);
                try {
                    return broadcast(n, requireBoolean(raw), factory);
                } finally {
                    Releasables.closeExpectNoException(raw);
                }
            }
        } finally {
            Releasables.closeExpectNoException(one);
        }
    }

    private static Block broadcast(int n, BooleanBlock oneRow, BlockFactory factory) {
        if (oneRow.isNull(0)) {
            return factory.newConstantNullBlock(n);
        }
        return factory.newConstantBooleanBlockWith(oneRow.getBoolean(oneRow.getFirstValueIndex(0)), n);
    }

    private Block evalOrdinals(int n, OrdinalBytesRefBlock ordinals) {
        if (n < MIN_POSITIONS) {
            return null;
        }
        BytesRefVector dict = ordinals.getDictionaryVector();
        int k = dict.getPositionCount();
        IntBlock ordsBlock = ordinals.getOrdinalsBlock();
        IntVector ordsVector = ordsBlock.asVector();
        boolean hasNullOrds = ordsVector == null && ordsBlock.mayHaveNulls();

        ReferencedOrds referenced = collectReferenced(n, k, ordsVector, ordsBlock);
        if (referenced == null) {
            return null;
        }
        int d = referenced.size();
        if (d == 0) {
            Boolean nr = nullResult();
            BlockFactory factory = driverContext.blockFactory();
            if (nr == null) {
                return factory.newConstantNullBlock(n);
            }
            return factory.newConstantBooleanBlockWith(nr, n);
        }

        long lookupBytes = lookupRamBytes(n, k, d);
        boolean charged = false;
        BytesRefVector copy = null;
        try {
            driverContext.breaker().addEstimateBytesAndMaybeBreak(lookupBytes, "dictionary-predicate-ords");
            charged = true;
            copy = copyReferenced(dict, referenced);
            try (Page dictPage = new Page(copy.asBlock())) {
                copy = null;
                Block raw;
                try {
                    raw = dictionary.eval(dictPage);
                } catch (IllegalArgumentException e) {
                    return null;
                }
                try {
                    return map(n, hasNullOrds, referenced, requireBoolean(raw));
                } finally {
                    Releasables.closeExpectNoException(raw);
                }
            }
        } finally {
            Releasables.closeExpectNoException(copy);
            if (charged) {
                driverContext.breaker().addWithoutBreaking(-lookupBytes);
            }
        }
    }

    private static BooleanBlock requireBoolean(Block raw) {
        if (raw instanceof BooleanBlock b) {
            return b;
        }
        throw new IllegalStateException("dictionary evaluator produced [" + (raw == null ? "null" : raw.elementType()) + "]");
    }

    private static long lookupRamBytes(int n, int k, int d) {
        long rowOrDict = k <= 2L * n ? (long) Integer.BYTES * k : (long) Integer.BYTES * n;
        return RamUsageEstimator.alignObjectSize(rowOrDict + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER) + RamUsageEstimator.alignObjectSize(
            (long) Integer.BYTES * d + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
        ) + 2L * d;
    }

    private BytesRefVector copyReferenced(BytesRefVector dict, ReferencedOrds referenced) {
        BlockFactory factory = driverContext.blockFactory();
        BytesRef scratch = new BytesRef();
        try (BytesRefVector.Builder builder = factory.newBytesRefVectorBuilder(referenced.size())) {
            for (int i = 0; i < referenced.size(); i++) {
                builder.appendBytesRef(dict.getBytesRef(referenced.ordAt(i), scratch));
            }
            return builder.build();
        }
    }

    private Block map(int n, boolean hasNullOrds, ReferencedOrds referenced, BooleanBlock dictResult) {
        int d = referenced.size();
        boolean[] vals = new boolean[d];
        boolean[] valNull = new boolean[d];
        boolean anyDictNull = false;
        boolean allSame = true;
        boolean haveCommon = false;
        boolean common = false;
        for (int i = 0; i < d; i++) {
            if (dictResult.isNull(i)) {
                valNull[i] = true;
                anyDictNull = true;
            } else {
                vals[i] = dictResult.getBoolean(dictResult.getFirstValueIndex(i));
                if (haveCommon == false) {
                    common = vals[i];
                    haveCommon = true;
                } else if (common != vals[i]) {
                    allSame = false;
                }
            }
        }
        if (anyDictNull) {
            allSame = false;
        }

        Boolean nr = hasNullOrds ? nullResult() : null;
        if (allSame && anyDictNull == false && haveCommon) {
            if (hasNullOrds == false || (nr != null && nr.booleanValue() == common)) {
                return driverContext.blockFactory().newConstantBooleanBlockWith(common, n);
            }
        }
        if (anyDictNull && haveCommon == false) {
            if (hasNullOrds == false || nr == null) {
                return driverContext.blockFactory().newConstantNullBlock(n);
            }
        }

        boolean outputHasNulls = anyDictNull || (hasNullOrds && nr == null);
        BlockFactory factory = driverContext.blockFactory();
        if (outputHasNulls == false) {
            try (BooleanVector.FixedBuilder out = factory.newBooleanVectorFixedBuilder(n)) {
                for (int i = 0; i < n; i++) {
                    int dense = referenced.denseAt(i);
                    if (dense < 0) {
                        if (nr == null) {
                            throw new IllegalStateException("unmapped ordinal at [" + i + "]");
                        }
                        out.appendBoolean(i, nr);
                    } else {
                        out.appendBoolean(i, vals[dense]);
                    }
                }
                return out.build().asBlock();
            }
        }
        try (BooleanBlock.Builder out = factory.newBooleanBlockBuilder(n)) {
            for (int i = 0; i < n; i++) {
                int dense = referenced.denseAt(i);
                if (dense < 0) {
                    if (nr == null) {
                        out.appendNull();
                    } else {
                        out.appendBoolean(nr);
                    }
                } else {
                    appendMapped(out, vals, valNull, dense);
                }
            }
            return out.build();
        }
    }

    private static void appendMapped(BooleanBlock.Builder out, boolean[] vals, boolean[] valNull, int dense) {
        if (valNull[dense]) {
            out.appendNull();
        } else {
            out.appendBoolean(vals[dense]);
        }
    }

    private Boolean nullResult() {
        if (nullResultComputed == false) {
            BlockFactory factory = driverContext.blockFactory();
            Block raw = null;
            try (Page p = new Page(factory.newConstantNullBlock(1))) {
                raw = dictionary.eval(p);
                BooleanBlock r = requireBoolean(raw);
                nullResult = r.isNull(0) ? null : r.getBoolean(r.getFirstValueIndex(0));
            } finally {
                Releasables.closeExpectNoException(raw);
            }
            nullResultComputed = true;
        }
        return nullResult;
    }

    private static ReferencedOrds collectReferenced(int n, int k, IntVector ordsVector, IntBlock ordsBlock) {
        int maxD = n / 2;
        if (k > 2L * n) {
            return collectWithHash(n, maxD, ordsVector, ordsBlock);
        }
        return collectWithBitset(n, k, maxD, ordsVector, ordsBlock);
    }

    private static ReferencedOrds collectWithBitset(int n, int k, int maxD, IntVector ordsVector, IntBlock ordsBlock) {
        long[] bits = new long[(k + 63) >>> 6];
        int d = 0;
        if (ordsVector != null) {
            for (int i = 0; i < n; i++) {
                d += setBit(bits, ordsVector.getInt(i));
                if (d > maxD) {
                    return null;
                }
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (ordsBlock.isNull(i) == false) {
                    d += setBit(bits, ordsBlock.getInt(ordsBlock.getFirstValueIndex(i)));
                    if (d > maxD) {
                        return null;
                    }
                }
            }
        }
        if (d == 0) {
            return ReferencedOrds.empty();
        }
        return compactFromBitset(bits, d, k, ordsVector, ordsBlock);
    }

    private static ReferencedOrds compactFromBitset(long[] bits, int d, int k, IntVector ordsVector, IntBlock ordsBlock) {
        int[] ords = new int[d];
        int[] ordToDense = new int[k];
        Arrays.fill(ordToDense, -1);
        int dense = 0;
        for (int word = 0; word < bits.length; word++) {
            long w = bits[word];
            while (w != 0) {
                int bit = Long.numberOfTrailingZeros(w);
                int ord = (word << 6) + bit;
                ords[dense] = ord;
                ordToDense[ord] = dense;
                dense++;
                w &= w - 1;
            }
        }
        return new ArrayReferencedOrds(ords, ordToDense, ordsVector, ordsBlock);
    }

    private static int setBit(long[] bits, int ord) {
        int word = ord >>> 6;
        long mask = 1L << (ord & 63);
        if ((bits[word] & mask) == 0) {
            bits[word] |= mask;
            return 1;
        }
        return 0;
    }

    private static ReferencedOrds collectWithHash(int n, int maxD, IntVector ordsVector, IntBlock ordsBlock) {
        IntIntHashMap map = new IntIntHashMap();
        int[] rowDense = new int[n];
        int[] ords = new int[maxD];
        int d = 0;
        if (ordsVector != null) {
            for (int i = 0; i < n; i++) {
                int ord = ordsVector.getInt(i);
                int index = map.indexOf(ord);
                if (index < 0) {
                    if (d == maxD) {
                        return null;
                    }
                    map.put(ord, d);
                    ords[d] = ord;
                    rowDense[i] = d;
                    d++;
                } else {
                    rowDense[i] = map.indexGet(index);
                }
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (ordsBlock.isNull(i)) {
                    rowDense[i] = -1;
                } else {
                    int ord = ordsBlock.getInt(ordsBlock.getFirstValueIndex(i));
                    int index = map.indexOf(ord);
                    if (index < 0) {
                        if (d == maxD) {
                            return null;
                        }
                        map.put(ord, d);
                        ords[d] = ord;
                        rowDense[i] = d;
                        d++;
                    } else {
                        rowDense[i] = map.indexGet(index);
                    }
                }
            }
        }
        if (d == 0) {
            return ReferencedOrds.empty();
        }
        return new RowDenseReferencedOrds(Arrays.copyOf(ords, d), rowDense);
    }

    private interface ReferencedOrds {
        int size();

        int ordAt(int dense);

        int denseAt(int position);

        static ReferencedOrds empty() {
            return new RowDenseReferencedOrds(new int[0], new int[0]);
        }
    }

    /**
     * Bitset path ({@code k ≤ 2n}): {@code ordToDense[k]} then per-row array load.
     */
    private record ArrayReferencedOrds(int[] ords, int[] ordToDense, IntVector ordsVector, IntBlock ordsBlock) implements ReferencedOrds {
        @Override
        public int size() {
            return ords.length;
        }

        @Override
        public int ordAt(int dense) {
            return ords[dense];
        }

        @Override
        public int denseAt(int position) {
            int ord;
            if (ordsVector != null) {
                ord = ordsVector.getInt(position);
            } else if (ordsBlock.isNull(position)) {
                return -1;
            } else {
                ord = ordsBlock.getInt(ordsBlock.getFirstValueIndex(position));
            }
            int dense = ordToDense[ord];
            if (dense < 0) {
                throw new IllegalStateException("unreferenced ordinal [" + ord + "]");
            }
            return dense;
        }
    }

    /**
     * Hash path ({@code k ≫ n}): remapped dense ids written during collection.
     */
    private record RowDenseReferencedOrds(int[] ords, int[] rowDense) implements ReferencedOrds {
        @Override
        public int size() {
            return ords.length;
        }

        @Override
        public int ordAt(int dense) {
            return ords[dense];
        }

        @Override
        public int denseAt(int position) {
            return rowDense[position];
        }
    }

    /** Test-only. Not part of {@code toString}. */
    public Path lastPath() {
        return lastPath;
    }

    @Override
    public long baseRamBytesUsed() {
        return BASE_RAM_BYTES_USED + perRow.baseRamBytesUsed() + dictionary.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(perRow, dictionary);
    }

    @Override
    public String toString() {
        return perRow.toString();
    }
}
