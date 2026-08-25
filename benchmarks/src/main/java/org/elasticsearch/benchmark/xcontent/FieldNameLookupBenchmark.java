/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.simdjson.fieldnames.FieldNameHash;
import org.elasticsearch.simdjson.fieldnames.FieldNameTable;
import org.elasticsearch.simdjson.fieldnames.FrozenFieldNameTable;
import org.elasticsearch.simdjson.fieldnames.FrozenNameTable;
import org.elasticsearch.simdjson.fieldnames.GphNameTable;
import org.elasticsearch.simdjson.fieldnames.PerfectHashNameTable;
import org.elasticsearch.simdjson.fieldnames.PrefixDirectMapTable;
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
 * Microbenchmark comparing field name resolution strategies.
 *
 * <p>Simulates the hot path: given a buffer of JSON field names (as they appear in a
 * parsed document), look up each name and return the canonical String. All tables
 * are pre-populated ("frozen") to measure steady-state lookup performance.
 *
 * <p>Two schemas are tested:
 * <ul>
 *   <li>{@code clickbench} — 90+ fields, many long names (typical ClickBench schema)</li>
 *   <li>{@code sparse} — 7 short fields (typical small document)</li>
 * </ul>
 */
@Fork(value = 1, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class FieldNameLookupBenchmark {

    @Param({ "clickbench", "sparse" })
    private String schema;

    private byte[][] fieldBytes;
    private int[] fieldOffsets;
    private int[] fieldLens;
    private int fieldCount;

    // Tables pre-populated for all strategies
    private FieldNameTable.Child currentChild;
    private FrozenNameTable frozenTable;
    private PerfectHashNameTable perfectHashTable;
    private GphNameTable gphTable;
    private PrefixDirectMapTable prefixTable;
    private FrozenFieldNameTable.Child frozenFieldChild;

    // Precomputed hashes for strategies that need them
    private int[] precomputedHashes;

    private static final String[] CLICKBENCH_FIELDS = {
        "WatchID",
        "JavaEnable",
        "Title",
        "GoodEvent",
        "EventTime",
        "EventDate",
        "CounterID",
        "ClientIP",
        "ClientIP6",
        "RegionID",
        "UserID",
        "CounterClass",
        "OS",
        "UserAgent",
        "URL",
        "Referer",
        "URLDomain",
        "RefererDomain",
        "Refresh",
        "IsRobot",
        "RefererCategories",
        "URLCategories",
        "URLRegions",
        "RefererRegions",
        "ResolutionWidth",
        "ResolutionHeight",
        "ResolutionDepth",
        "FlashMajor",
        "FlashMinor",
        "FlashMinor2",
        "NetMajor",
        "NetMinor",
        "UserAgentMajor",
        "UserAgentMinor",
        "CookieEnable",
        "JavascriptEnable",
        "IsMobile",
        "MobilePhone",
        "MobilePhoneModel",
        "Params",
        "IPNetworkID",
        "TraficSourceID",
        "SearchEngineID",
        "SearchPhrase",
        "AdvEngineID",
        "IsArtifical",
        "WindowClientWidth",
        "WindowClientHeight",
        "ClientTimeZone",
        "ClientEventTime",
        "SilverlightVersion1",
        "SilverlightVersion2",
        "SilverlightVersion3",
        "SilverlightVersion4",
        "PageCharset",
        "CodeVersion",
        "IsLink",
        "IsDownload",
        "IsNotBounce",
        "FUniqID",
        "HID",
        "IsOldCounter",
        "IsEvent",
        "IsParameter",
        "DontCountHits",
        "WithHash",
        "HitColor",
        "UTCEventTime",
        "Age",
        "Sex",
        "Income",
        "Interests",
        "Robotness",
        "GeneralInterests",
        "RemoteIP",
        "RemoteIP6",
        "WindowName",
        "OpenerName",
        "HistoryLength",
        "BrowserLanguage",
        "BrowserCountry",
        "SocialNetwork",
        "SocialAction",
        "HTTPError",
        "SendTiming",
        "DNSTiming",
        "ConnectTiming",
        "ResponseStartTiming",
        "ResponseEndTiming",
        "FetchTiming",
        "RedirectTiming",
        "DOMInteractiveTiming",
        "ContentLoadTiming",
        "OnLoadTiming",
        "RequestNum",
        "RequestTry",
        "NetErrorCode" };

    private static final String[] SPARSE_FIELDS = { "type", "id", "ts", "val", "label", "active", "count" };

    @Setup
    public void setUp() {
        String[] fieldNames = schema.equals("clickbench") ? CLICKBENCH_FIELDS : SPARSE_FIELDS;
        fieldCount = fieldNames.length;
        fieldBytes = new byte[fieldCount][];
        fieldOffsets = new int[fieldCount];
        fieldLens = new int[fieldCount];
        precomputedHashes = new int[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            byte[] raw = fieldNames[i].getBytes(StandardCharsets.UTF_8);
            // Pad to at least 8 extra bytes so wyhash readLE8/readSmall don't go out of bounds
            fieldBytes[i] = new byte[raw.length + 8];
            System.arraycopy(raw, 0, fieldBytes[i], 0, raw.length);
            fieldOffsets[i] = 0;
            fieldLens[i] = raw.length;
            precomputedHashes[i] = FieldNameHash.hashName(fieldBytes[i], 0, fieldLens[i]);
        }

        // Populate current FieldNameTable
        FieldNameTable root = new FieldNameTable();
        currentChild = root.makeChild();
        for (int i = 0; i < fieldCount; i++) {
            currentChild.lookupName(fieldBytes[i], 0, fieldLens[i]);
        }

        // Populate FrozenNameTable
        frozenTable = new FrozenNameTable();
        for (int i = 0; i < fieldCount; i++) {
            frozenTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]);
        }
        frozenTable.freeze();

        // Populate PerfectHashNameTable
        perfectHashTable = new PerfectHashNameTable();
        for (int i = 0; i < fieldCount; i++) {
            String name = fieldNames[i];
            perfectHashTable.insert(fieldBytes[i], 0, fieldLens[i], name);
        }
        perfectHashTable.freeze();
        // Verify
        for (int i = 0; i < fieldCount; i++) {
            String s = perfectHashTable.lookup(fieldBytes[i], 0, fieldLens[i]);
            if (s == null || !s.equals(fieldNames[i])) {
                throw new AssertionError("PerfectHash verification failed for: " + fieldNames[i] + ", got: " + s);
            }
        }

        // Populate GphNameTable
        gphTable = new GphNameTable();
        for (int i = 0; i < fieldCount; i++) {
            gphTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]);
        }
        gphTable.freeze();
        for (int i = 0; i < fieldCount; i++) {
            String s = gphTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]);
            if (s == null || !s.equals(fieldNames[i])) {
                throw new AssertionError("GPH verification failed for: " + fieldNames[i] + ", got: " + s);
            }
        }

        // Populate PrefixDirectMapTable
        prefixTable = new PrefixDirectMapTable();
        for (int i = 0; i < fieldCount; i++) {
            prefixTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]);
        }
        prefixTable.freeze();
        for (int i = 0; i < fieldCount; i++) {
            String s = prefixTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]);
            if (s == null || !s.equals(fieldNames[i])) {
                throw new AssertionError("PrefixDirect verification failed for: " + fieldNames[i] + ", got: " + s);
            }
        }

        // Populate FrozenFieldNameTable
        FrozenFieldNameTable frozenFieldRoot = new FrozenFieldNameTable();
        frozenFieldChild = frozenFieldRoot.makeChild();
        for (int i = 0; i < fieldCount; i++) {
            String s = frozenFieldChild.lookup(fieldBytes[i], 0, fieldLens[i], precomputedHashes[i]);
            if (s == null) {
                frozenFieldChild.insert(fieldBytes[i], 0, fieldLens[i], precomputedHashes[i]);
            }
        }
        frozenFieldChild.freeze();
        for (int i = 0; i < fieldCount; i++) {
            String s = frozenFieldChild.lookup(fieldBytes[i], 0, fieldLens[i], precomputedHashes[i]);
            if (s == null || !s.equals(fieldNames[i])) {
                throw new AssertionError("FrozenField verification failed for: " + fieldNames[i] + ", got: " + s);
            }
        }

        System.out.printf("[setup] schema=%s fields=%d%n", schema, fieldCount);
    }

    @Benchmark
    public void current_fieldNameTable(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(currentChild.lookupName(fieldBytes[i], 0, fieldLens[i]));
        }
    }

    @Benchmark
    public void frozen_compactTable(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(frozenTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]));
        }
    }

    @Benchmark
    public void frozen_compactTable_preHash(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(frozenTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i], precomputedHashes[i]));
        }
    }

    @Benchmark
    public void perfectHash_CHD(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(perfectHashTable.lookup(fieldBytes[i], 0, fieldLens[i]));
        }
    }

    @Benchmark
    public void perfectHash_GPH(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(gphTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]));
        }
    }

    @Benchmark
    public void prefixDirectMap(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(prefixTable.lookupOrInsert(fieldBytes[i], 0, fieldLens[i]));
        }
    }

    @Benchmark
    public void frozenField_compact(Blackhole bh) {
        for (int i = 0; i < fieldCount; i++) {
            bh.consume(frozenFieldChild.lookup(fieldBytes[i], 0, fieldLens[i], precomputedHashes[i]));
        }
    }
}
