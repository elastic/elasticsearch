/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.simdjson;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.simdjson.internal.fieldnames.FieldNameHash;
import org.elasticsearch.simdjson.internal.fieldnames.FrozenFieldNameTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Measures the field name cache paths in isolation, at the {@code FieldNameLookup} level rather
 * than through a full document parse. Driving this through {@code SimdJsonDirectWalker} would bury
 * the numbers inside overall parse throughput, and the cache itself is the question.
 *
 * <p>A frozen child consults its immutable hash table first and only then a small linear overflow
 * buffer holding names learned after freezing. So there are four paths worth separating:
 * <ul>
 *   <li>{@link #frozenHit} — the common case, and the one that must not regress. The overflow
 *       buffer is reached only after a frozen miss, so this should be flat across
 *       {@code overflowSize}; if it is not, the added fields have cost cache locality.</li>
 *   <li>{@link #overflowHit} — a frozen miss followed by an overflow hit. Costs a failed probe plus
 *       a scan, and allocates nothing. This path did not previously exist: the name was
 *       reallocated on every occurrence.</li>
 *   <li>{@link #absentLookup} — in neither structure, so the full scan runs and finds nothing.
 *       This is the sweep that prices the change: at {@code overflowSize=0} it is exactly the old
 *       miss cost, and the growth up to the cap is precisely what was added.</li>
 *   <li>{@link #uncachedResolve} — a miss resolved by allocating, on a child whose buffer is full.
 *       The allocation baseline to compare {@link #overflowHit} against.</li>
 * </ul>
 *
 * <p>Run with {@code -prof gc} and read {@code gc.alloc.rate.norm}: the allocation delta between
 * {@link #overflowHit} and {@link #uncachedResolve} is the crispest signal here, since allocation
 * counts carry far less noise than timings.
 *
 * <p>Every benchmark is non-mutating. That is a deliberate constraint rather than an accident: the
 * cache learns as it is used, so any benchmark that let it record during measurement would report
 * a blend of states rather than the one it set up.
 *
 * <pre>{@code
 * ./gradlew :libs:simdjson:benchmark --args "FieldNameCacheBenchmark \
 *   -prof gc -rf json -rff build/jmh-fieldnames.json"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class FieldNameCacheBenchmark {

    /** Names in the frozen table, i.e. how wide the first document was. */
    @Param({ "20", "90" })
    private int schemaSize;

    /**
     * Names held in the overflow buffer. Every frozen-table miss scans these, so this parameter is
     * what determines the added cost. Values beyond the buffer's cap saturate at the cap.
     */
    @Param({ "0", "8", "32", "64" })
    private int overflowSize;

    /** Enough inserts to fill the overflow buffer whatever its cap, so recording stops. */
    private static final int SATURATING_INSERTS = 4096;

    private FrozenFieldNameTable.Child child;
    private FrozenFieldNameTable.Child saturated;

    private byte[] frozenName;
    private int frozenHash;

    private byte[] overflowName;
    private int overflowHash;

    private byte[] absentName;
    private int absentHash;

    @Setup
    public void setUp() {
        BenchmarkLogging.configure();

        // Isolated tables rather than SimdJsonParserPool's global one, so each trial measures
        // exactly the state set up here. A child inherits the shared table once one exists and can
        // then no longer learn a schema, so the two children need tables of their own.
        child = frozenChild(new FrozenFieldNameTable(), overflowSize);
        saturated = frozenChild(new FrozenFieldNameTable(), SATURATING_INSERTS);

        frozenName = bytes("schema_field_" + (schemaSize - 1));
        frozenHash = FieldNameHash.hashName(frozenName, 0, frozenName.length);

        // The last recorded name, so overflowHit pays the whole scan rather than half of it. With
        // an empty buffer there is nothing to hit, and the benchmark degenerates to absentLookup.
        int lastRecorded = Math.max(0, Math.min(overflowSize, child.overflowCount()) - 1);
        overflowName = bytes("overflow_field_" + lastRecorded);
        overflowHash = FieldNameHash.hashName(overflowName, 0, overflowName.length);

        absentName = bytes("absent_field_name");
        absentHash = FieldNameHash.hashName(absentName, 0, absentName.length);
    }

    /** Builds a child frozen on {@code schemaSize} names, then adds {@code overflowInserts} more. */
    private FrozenFieldNameTable.Child frozenChild(FrozenFieldNameTable table, int overflowInserts) {
        FrozenFieldNameTable.Child c = table.makeChild();
        for (int i = 0; i < schemaSize; i++) {
            insert(c, "schema_field_" + i);
        }
        c.freeze();
        for (int i = 0; i < overflowInserts; i++) {
            insert(c, "overflow_field_" + i);
        }
        return c;
    }

    /** Frozen-table hit. Should be flat across {@code overflowSize}. */
    @Benchmark
    public void frozenHit(Blackhole bh) {
        bh.consume(child.lookup(frozenName, 0, frozenName.length, frozenHash));
    }

    /** Frozen miss then overflow hit: a failed probe plus a scan, and no allocation. */
    @Benchmark
    public void overflowHit(Blackhole bh) {
        bh.consume(child.lookup(overflowName, 0, overflowName.length, overflowHash));
    }

    /** Frozen miss then overflow miss. The sweep across {@code overflowSize} is the added cost. */
    @Benchmark
    public void absentLookup(Blackhole bh) {
        bh.consume(child.lookup(absentName, 0, absentName.length, absentHash));
    }

    /**
     * A miss resolved by allocating a name, which is what happens for every post-freeze name once
     * the overflow buffer is full. Non-mutating because the buffer is already saturated, so the
     * insert records nothing.
     */
    @Benchmark
    public void uncachedResolve(Blackhole bh) {
        bh.consume(saturated.lookup(absentName, 0, absentName.length, absentHash));
        bh.consume(saturated.insert(absentName, 0, absentName.length, absentHash));
    }

    private static void insert(FrozenFieldNameTable.Child child, String name) {
        byte[] buf = bytes(name);
        child.insert(buf, 0, buf.length, FieldNameHash.hashName(buf, 0, buf.length));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
