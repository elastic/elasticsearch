/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.flattened.ExtractFlattenedSubfieldConfig;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Microbenchmark for the three ways ES|QL reads a sub-field out of a {@code flattened} field, so the
 * relative cost of each can be quantified and any GA performance work on {@code field_extract} can be
 * measured against a stable baseline.
 * <ul>
 *   <li>{@code keyed_fused} - the fast path: {@code field_extract(root, "constant")} fuses into the
 *       {@link org.elasticsearch.index.mapper.flattened.KeyedFlattenedDocValuesBlockLoader}, reading a
 *       single sub-key's doc values directly. Selected by handing the mapper an
 *       {@link ExtractFlattenedSubfieldConfig} via the block-loader context.</li>
 *   <li>{@code root_then_evaluator} - the slow fallback: the whole flattened root is loaded as a JSON
 *       blob (as {@code root_only} does) and then {@link FieldExtract#processConstant} (the constant-key
 *       evaluator body production runs for a foldable key) re-parses that blob per row to pull out the
 *       key. This is {@code root_only}'s cost plus the per-row parse.</li>
 *   <li>{@code root_only} - the baseline: load the entire flattened object via
 *       {@link org.elasticsearch.index.mapper.flattened.RootFlattenedDocValuesBlockLoader} with no
 *       extraction, so {@code (root_then_evaluator - root_only)} isolates the per-row parse cost.</li>
 * </ul>
 * Both loaders read the same {@code field._keyed} doc values, so a single index build feeds all paths;
 * only the constructed {@link org.elasticsearch.index.mapper.BlockLoader} differs. The {@code subFields}
 * parameter controls how many sibling keys each document carries, which grows the root blob (and thus the
 * parse cost) while leaving the fused single-key read unchanged.
 *
 * <h2>Nightly scheduling</h2>
 * This class lives in the {@code org.elasticsearch.benchmark._nightly} package, which is the selector the
 * {@code periodic-micro-benchmarks} Buildkite pipeline runs ({@code :benchmarks:run --args
 * 'org.elasticsearch.benchmark._nightly ...'}); on {@code main} that job indexes the JSON results for the
 * performance dashboards. This benchmark is therefore picked up automatically, with no pipeline change,
 * once merged.
 * <p>
 * Because the job shares a single time budget across <em>every</em> {@code _nightly} benchmark, the
 * {@code @Param} <em>defaults</em> are deliberately trimmed to the regression-tracking essentials: the
 * {@code in_order} layout and the bare {@code sum} consumer, across all three {@code path}s and all
 * {@code subFields}. That 9-cell cross product isolates the fused-vs-fallback delta and its scaling in
 * object width. The diagnostic dimensions ({@code shuffled} layout, {@code dissect}/{@code grok} consumers)
 * add a constant per-row surcharge to every path and so do not move the tracked delta; they are dropped from
 * the scheduled defaults but remain available for ad-hoc runs via {@code -p} (e.g.
 * {@code -p layout=shuffled -p consumer=grok}) and are still exercised every fork by {@link #selfTest()},
 * which sweeps the full {@code SUPPORTED_*} matrix.
 * <p>
 * Note {@link Fork @Fork(1)}: single-fork trend lines carry no cross-fork variance, so treat a nightly delta
 * as indicative and confirm any suspected regression with a multi-fork re-run (see the profiling section).
 *
 * <h2>Measured baseline</h2>
 * Reference numbers from an {@code in_order} sweep on a quiet workstation ({@code keyed_fused} from a single
 * all-paths run; {@code root_*} from a 3-fork x 7-iteration run, {@code Cnt 21} per cell). Time is ns/op:
 * <pre>{@code
 * path                  subFields=5   subFields=20   subFields=100
 * keyed_fused                  ~65           ~69            ~81
 * root_only                  1422.4        5687.4        28570.4
 * root_then_evaluator        1568.1        6146.7        30930.2
 * }</pre>
 * Two facts drive the GA priorities:
 * <ul>
 *   <li><b>Root reconstruction is proportional to object width.</b> {@code root_only} fits ~286 ns per
 *       sub-field with a fixed cost indistinguishable from zero (linear, ~0 intercept). This whole-object
 *       JSON reconstruction is exactly the cost the fused path avoids by reading one sub-key's doc values.</li>
 *   <li><b>The per-row parse is a small, also-linear surcharge, not the bottleneck.</b> The evaluator cost
 *       {@code (root_then_evaluator - root_only)} is ~8-10% (~23 ns per sub-field), so the fallback is
 *       reconstruction-bound, not parse-bound.</li>
 * </ul>
 * At {@code subFields=100} the fallback is ~350x the fused read (~81 ns, flat in width) and the gap widens
 * linearly, so GA effort should widen the set of query shapes that hit the fused path rather than optimize
 * the parse. See the profiling recipes below to reproduce and attribute these numbers.
 *
 * <h2>Consumer invariance (DISSECT / GROK)</h2>
 * The {@code consumer} parameter runs a real downstream parser on each extracted value, modeling the commands that
 * consume a {@code field_extract(...)} input once it is fused into their load. The fused-vs-fallback gap is unchanged
 * by the consumer because the parser runs identically on both paths &mdash; e.g. under {@code consumer=dissect} at
 * {@code subFields=100}, {@code keyed_fused} is ~144 ns/op while {@code root_then_evaluator} is ~32,000 ns/op
 * (~225x). The parser adds a flat per-row surcharge (~60 ns here) to <em>both</em> paths, so widening the set of
 * shapes that reach the fused path &mdash; such as {@code DISSECT}/{@code GROK} over a flattened field &mdash; wins
 * the whole reconstruction gap per row, regardless of what consumes the value.
 *
 * <h2>Fusion coverage (which query shapes reach the fused path)</h2>
 * The lever above &mdash; "widen the set of shapes that reach the fused path" &mdash; is, as of GA, largely spent.
 * A structural cross-check of every expression-bearing physical node shows the fused path already covers
 * everything that evaluates {@code field_extract} over local rows. {@code PushExpressionsToFieldLoad} dispatches
 * directly on {@code EvalExec}, {@code FilterExec}, {@code AggregateExec}, {@code RegexExtractExec}
 * ({@code DISSECT}/{@code GROK}) and {@code CompoundOutputEvalExec} ({@code URI_PARTS}/{@code REGISTERED_DOMAIN}/
 * {@code USER_AGENT}/{@code IP_LOCATION}). Sort keys, {@code LIMIT}/{@code CHANGE_POINT} groupings and
 * {@code STATS} grouping/agg expressions need no dispatch of their own: the {@code Replace*ExpressionWithEval}
 * logical rules hoist any non-attribute child into a preceding {@code EVAL} first (e.g.
 * {@code SORT field_extract(f,"k")} becomes {@code EVAL t = field_extract(f,"k") | SORT t}), so the
 * {@code EvalExec} case picks them up transitively. The only expression positions left un-fused sit on the
 * inference/rerank nodes ({@code CompletionExec.prompt}, {@code RerankExec} fields), and fusing there is
 * pointless: those commands make a per-invocation inference round-trip that dwarfs the reconstruction cost this
 * benchmark measures. The one remaining <em>addressable</em> gap is the above-join / multi-source gate
 * ({@code Primaries.canPush} requires a single source), a correctness constraint rather than a missing shape.
 * Net: for shapes that read a flattened key locally, the {@code keyed_fused} column here is the steady state,
 * and the fallback columns only survive where fusion is intentionally withheld.
 *
 * <h2>Running and profiling</h2>
 * Everything inside {@code --args '...'} is passed straight to JMH. During profiling add
 * {@code -jvmArgsAppend -DskipSelfTest=true}: {@link #selfTest()} runs in every fork's static initializer and
 * the measured method already checksums correctness, so skipping it keeps each fork fast. On macOS
 * {@code -prof perf}/{@code perfasm} are unavailable (Linux only); use {@code -prof gc} and {@code -prof async}.
 *
 * <p><b>1. Allocation profile (cheap; confirms the root reconstruction is O(subFields)).</b> The key column is
 * {@code gc.alloc.rate.norm} (bytes/op): it should scale ~linearly across {@code subFields} for the {@code root_*}
 * paths and stay flat and tiny for {@code keyed_fused}; {@code root_only} and {@code root_then_evaluator} should
 * allocate nearly the same, showing the per-row parse adds little.</p>
 * <pre>{@code
 * ./gradlew -p benchmarks run --args 'FlattenedFieldExtractBenchmark -p layout=in_order -prof gc -f 1 -jvmArgsAppend -DskipSelfTest=true'
 * }</pre>
 *
 * <p><b>2. Async flamegraphs (the attribution: reconstruction vs parse).</b> Needs async-profiler 4.0
 * (point {@code libPath} at its {@code libasyncProfiler.so}; on macOS use {@code libasyncProfiler.dylib} and
 * {@code event=cpu}, falling back to {@code event=itimer}). Profile the two root cells at the widest object
 * size separately and diff them. A measured {@code subFields=100} run shows both paths are dominated by the
 * root loader ({@code RootFlattenedDocValuesBlockLoader...writeToBlock} &asymp; 99% inclusive), and within it the
 * cost is <em>doc-values term resolution</em>, not JSON serialization: {@code SortedSetFlattenedDocValues.next}
 * &rarr; {@code Lucene90DocValuesProducer...lookupOrd} is &asymp; 80% (self time lands in {@code TermsDict.next}
 * / {@code seekExact}, {@code LZ4.decompress}, and byte copies out of the compressed terms dictionary - one
 * ordinal&rarr;term lookup per sub-field per document), while assembling the JSON blob
 * ({@code XContentBuilder.field} / Jackson {@code writeStringField}) is only &asymp; 8-10%. {@code FieldExtract#processConstant}
 * (the per-row re-parse) appears only as a thin {@code UTF8StreamJsonParser} slice in {@code root_then_evaluator},
 * which carried &asymp; 6% more total samples than {@code root_only} - direct evidence the fallback is
 * reconstruction bound (specifically term-lookup bound), not parse bound. The corollary for GA: the lever on the
 * fallback is fewer/cheaper {@code lookupOrd}s, not the JSON parser; better still, widen the fused path so the
 * root is never reconstructed.</p>
 * <pre>{@code
 * ./gradlew -p benchmarks run --args 'FlattenedFieldExtractBenchmark.benchmark -p path=root_only -p layout=in_order -p subFields=100 -f 1 -jvmArgsAppend -DskipSelfTest=true -prof "async:libPath=/ABS/PATH/libasyncProfiler.so;dir=/tmp/prof-rootonly;output=flamegraph"'
 * ./gradlew -p benchmarks run --args 'FlattenedFieldExtractBenchmark.benchmark -p path=root_then_evaluator -p layout=in_order -p subFields=100 -f 1 -jvmArgsAppend -DskipSelfTest=true -prof "async:libPath=/ABS/PATH/libasyncProfiler.so;dir=/tmp/prof-rooteval;output=flamegraph"'
 * }</pre>
 *
 * <p><b>3. Explain the fused layout penalty (why {@code shuffled} is ~2x {@code in_order}).</b> Expect the
 * fused stack to be {@code KeyedFlattenedDocValuesBlockLoader} doing {@code SortedSetDocValues} ordinal
 * advance/lookup; the {@code shuffled} run spends more time in random doc/ordinal access, so it is doc-values
 * bound rather than extraction bound.</p>
 * <pre>{@code
 * ./gradlew -p benchmarks run --args 'FlattenedFieldExtractBenchmark.benchmark -p path=keyed_fused -p subFields=100 -f 1 -jvmArgsAppend -DskipSelfTest=true -prof "async:libPath=/ABS/PATH/libasyncProfiler.so;dir=/tmp/prof-fused;output=flamegraph"'
 * }</pre>
 *
 * <p><b>4. Trustworthy headline numbers.</b> {@link Fork @Fork(1)} captures no cross-fork variance, so re-run
 * the final numbers with more forks (and watch the {@code Error} column) on a quiet machine.</p>
 * <pre>{@code
 * ./gradlew -p benchmarks run --args 'FlattenedFieldExtractBenchmark -f 3'
 * }</pre>
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class FlattenedFieldExtractBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = "field";
    private static final String KEYED_FIELD = FIELD + "._keyed";
    private static final String SEPARATOR = "\0";
    /** The sub-key extracted by the {@code keyed_fused} and {@code root_then_evaluator} paths. */
    private static final String KEY = "sub_0";

    /**
     * Handle to the package-private {@link FieldExtract#processConstant} - the per-row evaluator body that
     * production runs for a <em>foldable</em> key (via {@code FieldExtractConstantEvaluator}; see
     * {@link FieldExtract#toEvaluator}). The {@code root_then_evaluator} path models
     * {@code field_extract(root, "sub_0")}, whose key is a constant, so it must call this rather than the
     * public {@link FieldExtract#process} - the latter re-derives the key with {@code BytesRef#utf8ToString}
     * and re-runs {@code validateFieldExtractPath} on every row, work the constant path does once at plan
     * time. Bound once into a {@code static final} so the JIT can inline {@code invokeExact}, keeping the
     * measured fallback cost faithful to production instead of overstating it (most visibly for small
     * {@code subFields}, where the whole-blob parse is cheap enough for that per-row key work to matter).
     */
    private static final MethodHandle PROCESS_CONSTANT;
    static {
        try {
            PROCESS_CONSTANT = MethodHandles.privateLookupIn(FieldExtract.class, MethodHandles.lookup())
                .findStatic(
                    FieldExtract.class,
                    "processConstant",
                    MethodType.methodType(void.class, BytesRefBlock.Builder.class, BytesRef.class, String.class)
                );
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final String[] SUPPORTED_LAYOUTS = new String[] { "in_order", "shuffled" };
    private static final String[] SUPPORTED_PATHS = new String[] { "keyed_fused", "root_then_evaluator", "root_only" };
    private static final String[] SUPPORTED_CONSUMERS = new String[] { "sum", "dissect", "grok" };
    private static final int[] SUPPORTED_SUB_FIELDS = new int[] { 5, 20, 100 };

    /**
     * Downstream consumers applied to the extracted keyword value. The point is that the consumer runs
     * <em>identically</em> whether the value arrived via the fused keyed loader ({@code keyed_fused}) or via the
     * root-load-then-reparse fallback ({@code root_then_evaluator}): {@code dissect}/{@code grok} model the
     * {@code DISSECT}/{@code GROK} commands that consume a {@code field_extract(...)} input, so the fused-vs-fallback
     * gap this benchmark reports is exactly the per-row win those commands gain from fusion. Each pattern captures the
     * whole integer token into {@code v}, so every consumer yields the same order-independent checksum and the existing
     * self-test validates them all for free.
     */
    private static final DissectParser DISSECT_PARSER = new DissectParser("%{v}", "");
    private static final Grok GROK = new Grok(GrokBuiltinPatterns.get(true), "%{NUMBER:v}", w -> {});

    private static final int BLOCK_LENGTH = 16 * 1024;
    private static final int INDEX_SIZE = 5 * BLOCK_LENGTH;
    /**
     * A tiny index size used only by {@link #selfTest()}. The self-test runs the whole
     * {@code layout x path x subFields} correctness matrix inside every JMH fork, so using the full
     * {@link #INDEX_SIZE} there makes the {@code root_*} paths (which reconstruct the entire flattened
     * object per document) dominate fork startup. A few thousand docs still exercises multiple segments
     * and the {@code i % VALUE_MOD} checksum wraparound while keeping each fork's self-test near-instant.
     */
    private static final int SELF_TEST_INDEX_SIZE = 2048;
    private static final int COMMIT_INTERVAL = 500;
    /** The extracted value for a document is {@code i % VALUE_MOD}, so the checksum is order-independent. */
    private static final int VALUE_MOD = 1000;
    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public static IndexSettings defaultIndexSettings() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }

    static {
        // Smoke test all the expected values and force loading subclasses more like prod.
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    static void selfTest() {
        try {
            FlattenedFieldExtractBenchmark benchmark = new FlattenedFieldExtractBenchmark();
            benchmark.indexSize = SELF_TEST_INDEX_SIZE;
            for (int subFields : SUPPORTED_SUB_FIELDS) {
                benchmark.subFields = subFields;
                benchmark.setupIndex();
                try {
                    for (String layout : SUPPORTED_LAYOUTS) {
                        benchmark.layout = layout;
                        benchmark.setupPages();
                        for (String consumer : SUPPORTED_CONSUMERS) {
                            benchmark.consumer = consumer;
                            for (String path : SUPPORTED_PATHS) {
                                benchmark.path = path;
                                try {
                                    benchmark.benchmark();
                                } catch (Exception e) {
                                    throw new AssertionError(
                                        "error initializing [" + layout + "/" + path + "/" + consumer + "/" + subFields + "]",
                                        e
                                    );
                                }
                            }
                        }
                    }
                } finally {
                    benchmark.teardownIndex();
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Layouts for the input blocks, matching {@link ValuesSourceReaderBenchmark}: {@code in_order} is how
     * {@link LuceneSourceOperator} emits docs for the most efficient read, while {@code shuffled} models a
     * large-block {@link TopNOperator}-style out-of-order read. The scheduled default is {@code in_order}
     * only (see "Nightly scheduling"); run {@code shuffled} ad-hoc with {@code -p layout=shuffled}.
     */
    @Param({ "in_order" })
    public String layout;

    @Param({ "keyed_fused", "root_then_evaluator", "root_only" })
    public String path;

    /**
     * Downstream work applied to each extracted value. {@code sum} is the bare loader-cost baseline; {@code dissect}
     * and {@code grok} run the real {@code DISSECT}/{@code GROK} parsers, mirroring the commands that now fuse a
     * {@code field_extract(...)} input. Ignored by {@code root_only}, which extracts no value. The scheduled default
     * is {@code sum} only (the parsers add a flat surcharge to every path, so they do not move the fused-vs-fallback
     * delta - see "Nightly scheduling"); run them ad-hoc with {@code -p consumer=dissect} / {@code -p consumer=grok}.
     */
    @Param({ "sum" })
    public String consumer;

    @Param({ "5", "20", "100" })
    public int subFields;

    private Directory directory;
    private IndexReader reader;
    private List<Page> pages;
    private FlattenedFieldMapper.RootFlattenedFieldType fieldType;
    /** Number of indexed docs. {@link #INDEX_SIZE} for measured runs; {@link #SELF_TEST_INDEX_SIZE} for the self-test. */
    private int indexSize = INDEX_SIZE;

    @Benchmark
    @OperationsPerInvocation(INDEX_SIZE)
    public void benchmark() {
        BlockLoaderFunctionConfig config = path.equals("keyed_fused") ? new ExtractFlattenedSubfieldConfig(KEY) : null;
        ElementType elementType = ElementType.BYTES_REF;
        List<ValuesSourceReaderOperator.FieldInfo> fields = List.of(
            new ValuesSourceReaderOperator.FieldInfo(
                FIELD,
                elementType,
                false,
                (ctx, shardIdx) -> ValuesSourceReaderOperator.load(fieldType.blockLoader(new BenchContext(config)))
            )
        );
        boolean reuseColumnLoaders = fields.size() <= PlannerSettings.REUSE_COLUMN_LOADERS_THRESHOLD.get(Settings.EMPTY);
        ValuesSourceReaderOperator op = new ValuesSourceReaderOperator(
            new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null),
            ByteSizeValue.ofMb(1).getBytes(),
            fields,
            new IndexedByShardIdFromSingleton<>(new ValuesSourceReaderOperator.ShardContext(reader, (sourcePaths) -> {
                throw new UnsupportedOperationException("can't load _source here");
            }, EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(Settings.EMPTY))),
            reuseColumnLoaders,
            0,
            PlannerSettings.SOURCE_RESERVATION_FACTOR.getDefault(Settings.EMPTY),
            PlannerSettings.DOC_SEQUENCE_BYTES_REF_FIELD_THRESHOLD.getDefault(Settings.EMPTY),
            () -> 0L
        );
        long sum = 0;
        long positions = 0;
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            op.addInput(page.shallowCopy());
            BytesRefBlock loaded = op.getOutput().<BytesRefBlock>getBlock(1);
            switch (path) {
                case "keyed_fused" -> sum += sumSingleValued(loaded, scratch);
                case "root_only" -> positions += loaded.getPositionCount();
                case "root_then_evaluator" -> sum += extractAndSum(loaded, scratch);
            }
        }
        switch (path) {
            case "keyed_fused", "root_then_evaluator" -> {
                long expected = 0;
                for (int i = 0; i < indexSize; i++) {
                    expected += i % VALUE_MOD;
                }
                if (expected != sum) {
                    throw new AssertionError(
                        "[" + layout + "][" + path + "][" + subFields + "] expected [" + expected + "] but was [" + sum + "]"
                    );
                }
            }
            case "root_only" -> {
                if (positions != indexSize) {
                    throw new AssertionError(
                        "["
                            + layout
                            + "]["
                            + path
                            + "]["
                            + subFields
                            + "] expected ["
                            + indexSize
                            + "] positions but was ["
                            + positions
                            + "]"
                    );
                }
            }
        }
    }

    /**
     * Sums the integer value at every position of a single-valued keyword block. Used by the fused path,
     * whose loader emits the extracted sub-key directly.
     */
    private long sumSingleValued(BytesRefBlock block, BytesRef scratch) {
        long sum = 0;
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                throw new AssertionError("unexpected null at position [" + p + "]");
            }
            sum += consume(block.getBytesRef(block.getFirstValueIndex(p), scratch).utf8ToString());
        }
        return sum;
    }

    /**
     * Applies the selected {@link #consumer} to one extracted value and returns the integer it carries. Every
     * consumer captures the whole token into {@code v}, so the returned value is identical across consumers and the
     * per-row cost difference is purely the parser's. Shared by the fused and fallback extract paths so the consumer
     * is provably invariant to how the value was loaded.
     */
    private long consume(String value) {
        switch (consumer) {
            case "sum" -> {
                return Integer.parseInt(value);
            }
            case "dissect" -> {
                Map<String, String> captures = DISSECT_PARSER.parse(value);
                return Integer.parseInt(captures.get("v"));
            }
            case "grok" -> {
                Map<String, Object> captures = GROK.captures(value);
                if (captures == null) {
                    throw new AssertionError("grok pattern did not match [" + value + "]");
                }
                return Integer.parseInt(captures.get("v").toString());
            }
            default -> throw new IllegalArgumentException("unsupported consumer [" + consumer + "]");
        }
    }

    /**
     * Runs the per-row fallback evaluator ({@link FieldExtract#processConstant}, the constant-key body
     * production uses for a foldable key) over a block of whole flattened JSON blobs, mirroring what the
     * slow path does after the root loader materializes each document.
     */
    private long extractAndSum(BytesRefBlock rootBlobs, BytesRef scratch) {
        long sum = 0;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(rootBlobs.getPositionCount())) {
            for (int p = 0; p < rootBlobs.getPositionCount(); p++) {
                BytesRef blob = rootBlobs.getBytesRef(rootBlobs.getFirstValueIndex(p), scratch);
                try {
                    PROCESS_CONSTANT.invokeExact(builder, blob, KEY);
                } catch (Throwable t) {
                    throw new AssertionError("processConstant failed", t);
                }
            }
            try (BytesRefBlock extracted = builder.build()) {
                BytesRef valueScratch = new BytesRef();
                for (int p = 0; p < extracted.getPositionCount(); p++) {
                    if (extracted.isNull(p)) {
                        throw new AssertionError("unexpected null at position [" + p + "]");
                    }
                    sum += consume(extracted.getBytesRef(extracted.getFirstValueIndex(p), valueScratch).utf8ToString());
                }
            }
        }
        return sum;
    }

    @Setup
    public void setup() throws IOException {
        setupIndex();
        setupPages();
    }

    private void setupIndex() throws IOException {
        IndexSettings indexSettings = defaultIndexSettings();
        fieldType = new FlattenedFieldMapper.Builder(FIELD, indexSettings).build(MapperBuilderContext.root(false, false)).fieldType();

        directory = new ByteBuffersDirectory();
        try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < indexSize; i++) {
                List<SortedSetDocValuesField> doc = new ArrayList<>(subFields);
                String value = Integer.toString(i % VALUE_MOD);
                for (int k = 0; k < subFields; k++) {
                    // "key\0value" is exactly how FlattenedFieldParser stores each leaf in the keyed channel.
                    doc.add(new SortedSetDocValuesField(KEYED_FIELD, new BytesRef("sub_" + k + SEPARATOR + value)));
                }
                iw.addDocument(doc);
                if (i % COMMIT_INTERVAL == 0) {
                    iw.commit();
                }
            }
        }
        reader = DirectoryReader.open(directory);
    }

    private void setupPages() {
        pages = new ArrayList<>();
        switch (layout) {
            case "in_order" -> {
                IntVector.Builder docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                for (LeafReaderContext ctx : reader.leaves()) {
                    int begin = 0;
                    while (begin < ctx.reader().maxDoc()) {
                        int end = Math.min(begin + BLOCK_LENGTH, ctx.reader().maxDoc());
                        for (int doc = begin; doc < end; doc++) {
                            docs.appendInt(doc);
                        }
                        pages.add(
                            new Page(
                                new DocVector(
                                    AlwaysReferencedIndexedByShardId.INSTANCE,
                                    blockFactory.newConstantIntBlockWith(0, end - begin).asVector(),
                                    blockFactory.newConstantIntBlockWith(ctx.ord, end - begin).asVector(),
                                    docs.build(),
                                    DocVector.config().singleSegmentNonDecreasing(true)
                                ).asBlock()
                            )
                        );
                        docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                        begin = end;
                    }
                }
            }
            case "shuffled" -> {
                record ItrAndOrd(PrimitiveIterator.OfInt itr, int ord) {}
                List<ItrAndOrd> docItrs = new ArrayList<>(reader.leaves().size());
                for (LeafReaderContext ctx : reader.leaves()) {
                    docItrs.add(new ItrAndOrd(IntStream.range(0, ctx.reader().maxDoc()).iterator(), ctx.ord));
                }
                IntVector.Builder docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                IntVector.Builder leafs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                int size = 0;
                while (docItrs.isEmpty() == false) {
                    Iterator<ItrAndOrd> itrItr = docItrs.iterator();
                    while (itrItr.hasNext()) {
                        ItrAndOrd next = itrItr.next();
                        if (false == next.itr.hasNext()) {
                            itrItr.remove();
                            continue;
                        }
                        docs.appendInt(next.itr.nextInt());
                        leafs.appendInt(next.ord);
                        size++;
                        if (size >= BLOCK_LENGTH) {
                            pages.add(
                                new Page(
                                    new DocVector(
                                        AlwaysReferencedIndexedByShardId.INSTANCE,
                                        blockFactory.newConstantIntVector(0, size),
                                        leafs.build(),
                                        docs.build(),
                                        DocVector.config()
                                    ).asBlock()
                                )
                            );
                            docs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                            leafs = blockFactory.newIntVectorBuilder(BLOCK_LENGTH);
                            size = 0;
                        }
                    }
                }
                if (size > 0) {
                    pages.add(
                        new Page(
                            new DocVector(
                                AlwaysReferencedIndexedByShardId.INSTANCE,
                                blockFactory.newConstantIntBlockWith(0, size).asVector(),
                                leafs.build().asBlock().asVector(),
                                docs.build(),
                                DocVector.config()
                            ).asBlock()
                        )
                    );
                }
            }
            default -> throw new IllegalArgumentException("unsupported layout [" + layout + "]");
        }
    }

    @TearDown
    public void teardownIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    private static class BenchContext implements MappedFieldType.BlockLoaderContext {
        private final BlockLoaderFunctionConfig functionConfig;

        BenchContext(BlockLoaderFunctionConfig functionConfig) {
            this.functionConfig = functionConfig;
        }

        @Override
        public String indexName() {
            return "benchmark";
        }

        @Override
        public IndexSettings indexSettings() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
            return MappedFieldType.FieldExtractPreference.NONE;
        }

        @Override
        public SearchLookup lookup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> sourcePaths(String name) {
            return Set.of(name);
        }

        @Override
        public String parentField(String field) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
            return FieldNamesFieldMapper.FieldNamesFieldType.get(true);
        }

        @Override
        public MappingLookup mappingLookup() {
            return null;
        }

        @Override
        public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
            return functionConfig;
        }

        @Override
        public Warnings warnings() {
            return null;
        }

        @Override
        public ByteSizeValue ordinalsByteSize() {
            return DEFAULT_ORDINALS_BYTE_SIZE;
        }

        @Override
        public ByteSizeValue scriptByteSize() {
            return DEFAULT_SCRIPT_BYTE_SIZE;
        }
    }
}
