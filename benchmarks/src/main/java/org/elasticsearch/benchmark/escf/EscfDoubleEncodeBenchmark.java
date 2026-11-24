/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures what {@link EscfEncoder} pays for a floating-point value of a JSON document, from the source bytes of
 * a bulk to the finished batch.
 *
 * <p>The encoder keeps the spelling of a floating-point value next to its double whenever the spelling is not
 * what {@code Double.toString} renders for that double, so that string consumers index what the client wrote.
 * To know whether it has to, it compares every floating-point token with that rendering, also the tokens that
 * end up storing nothing. {@code spelling} therefore separates the two costs:
 * <ul>
 *   <li>{@code canonicalShort}: a few digits, spelled as {@code Double.toString} spells them ({@code 23.5},
 *       {@code 0.875}). Pays the comparison and stores nothing.</li>
 *   <li>{@code canonicalLong}: the same with 16 or 17 digits ({@code 731.0585787324722}), which is what a
 *       client that serializes computed doubles sends.</li>
 *   <li>{@code twoDecimals}: amounts with two decimals. One in ten ends in a zero ({@code 19.90}) and has its
 *       spelling stored, so the column carries a spellings sidecar that is mostly empty entries.</li>
 *   <li>{@code trailingZero}: every value ends in a zero, so every value has its spelling stored.</li>
 * </ul>
 *
 * <p>{@code path} selects the walk, since each one finds the spelling differently: {@code simdjson} reads the
 * token bytes in place and {@code jackson} gets the token as a {@code String}.
 *
 * <p>The score is per value: an invocation encodes {@value #DOCS} documents of {@value #FIELDS} floating-point
 * fields each into one batch. Pages come from a real {@link PageCacheRecycler}, as in the indexing path, so
 * {@code gc.alloc.rate.norm} is the garbage a value leaves behind rather than the pages of the batch.
 *
 * <p>The fork runs with a fixed heap and a fixed young generation. Nothing but the recycler's pages survives
 * an invocation, so every collection is a young one, and with the young generation pinned {@code gc.count}
 * and {@code gc.time} of {@code -prof gc} are comparable between two runs of this benchmark.
 *
 * <h2>Ready to run</h2>
 *
 * <pre>{@code
 * # Time, allocation and collections per value, both walks
 * ./gradlew :benchmarks:run --args="EscfDoubleEncodeBenchmark -prof gc"
 *
 * # One walk, one spelling
 * ./gradlew :benchmarks:run --args="EscfDoubleEncodeBenchmark -prof gc -p path=simdjson -p spelling=canonicalShort"
 *
 * # Quick smoke
 * ./gradlew :benchmarks:run --args="EscfDoubleEncodeBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1 -p spelling=twoDecimals"
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 3, jvmArgsPrepend = { "-Xms2g", "-Xmx2g", "-Xmn512m" })
@State(Scope.Thread)
public class EscfDoubleEncodeBenchmark {

    static {
        // The encoder logs; without a logger provider its class fails to initialize in the fork.
        BenchmarkLogging.configure();
    }

    static final int DOCS = 5000;
    static final int FIELDS = 8;

    @Param({ "simdjson", "jackson" })
    public String path;

    @Param({ "canonicalShort", "canonicalLong", "twoDecimals", "trailingZero" })
    public String spelling;

    private Recycler<BytesRef> recycler;
    private boolean allowSimd;
    private BytesReference[] docs;

    @Setup(Level.Trial)
    public void setUp() {
        recycler = new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY));
        allowSimd = switch (path) {
            case "simdjson" -> {
                if (EscfEncoder.isSimdEnabled() == false) {
                    // The encoder would quietly take the Jackson walk and this would report it as simdjson.
                    throw new IllegalStateException("the simdjson walk is not available in this JVM, run through :benchmarks:run");
                }
                yield true;
            }
            case "jackson" -> false;
            default -> throw new IllegalArgumentException("unknown path [" + path + "]");
        };

        Random random = new Random(17);
        docs = new BytesReference[DOCS];
        int stored = 0;
        for (int doc = 0; doc < DOCS; doc++) {
            String[] tokens = new String[FIELDS];
            for (int field = 0; field < FIELDS; field++) {
                tokens[field] = token(random);
                if (isStored(tokens[field])) {
                    stored++;
                }
            }
            docs[doc] = json(tokens);
        }
        checkStored(stored);
    }

    @Benchmark
    @OperationsPerInvocation(DOCS * FIELDS)
    public void encode(Blackhole bh) throws IOException {
        try (EscfEncoder encoder = new EscfEncoder(recycler, allowSimd)) {
            for (BytesReference doc : docs) {
                encoder.addDocument(doc, XContentType.JSON, 0);
            }
            try (EscfBatch batch = encoder.buildPartition(0)) {
                bh.consume(batch);
            }
        }
    }

    private String token(Random random) {
        return switch (spelling) {
            case "canonicalShort" -> {
                int scale = switch (random.nextInt(3)) {
                    case 0 -> 10;
                    case 1 -> 100;
                    default -> 1000;
                };
                yield Double.toString(random.nextInt(1000 * scale) / (double) scale);
            }
            case "canonicalLong" -> Double.toString(random.nextDouble() * 1000);
            case "twoDecimals" -> {
                int cents = random.nextInt(100_000);
                yield cents / 100 + "." + cents % 100 / 10 + cents % 10;
            }
            case "trailingZero" -> random.nextInt(1000) + "." + random.nextInt(10) + "0";
            default -> throw new IllegalArgumentException("unknown spelling [" + spelling + "]");
        };
    }

    /** Whether the encoder has to store the spelling of {@code token}. */
    private static boolean isStored(String token) {
        return Double.toString(Double.parseDouble(token)).equals(token) == false;
    }

    /** Fails the trial when the documents do not hold the mix of spellings that the parameter promises. */
    private void checkStored(int stored) {
        int values = DOCS * FIELDS;
        boolean asPromised = switch (spelling) {
            case "canonicalShort", "canonicalLong" -> stored == 0;
            // One amount in ten ends in a zero.
            case "twoDecimals" -> stored > values / 20 && stored < values / 5;
            case "trailingZero" -> stored == values;
            default -> throw new IllegalArgumentException("unknown spelling [" + spelling + "]");
        };
        if (asPromised == false) {
            throw new IllegalStateException("[" + spelling + "] stores " + stored + " of " + values + " spellings");
        }
    }

    private static BytesReference json(String[] tokens) {
        StringBuilder doc = new StringBuilder("{");
        for (int field = 0; field < tokens.length; field++) {
            if (field > 0) {
                doc.append(',');
            }
            doc.append("\"d").append(field).append("\":").append(tokens[field]);
        }
        return new BytesArray(doc.append('}').toString().getBytes(StandardCharsets.UTF_8));
    }
}
