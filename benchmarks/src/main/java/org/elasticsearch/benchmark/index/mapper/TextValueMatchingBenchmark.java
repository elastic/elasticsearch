/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.index.mapper.CandidateValues;
import org.elasticsearch.index.mapper.TokenStreamMatching;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * How fast a text query is answered from a field's values, by each of the three ways one value can be matched.
 *
 * <p><b>The score is MiB of text a second, per thread</b>: one invocation matches exactly {@value #TEXT_PER_INVOCATION}
 * bytes of values, so the throughput JMH reports reads directly as MiB/s. Pass {@code -t N} to measure several
 * threads, where the score becomes their total.
 *
 * <p>It measures <b>matching alone, over values already in memory</b>: nothing here decompresses a column or reads a
 * disk. Those are their own throughputs — a cold read and a decode — and the number here is the ceiling they feed.
 *
 * <ul>
 *   <li>{@link #memoryIndex} — the value's tokens indexed into a {@link MemoryIndex} and the query run over it.</li>
 *   <li>{@link #tokenStream} — the tokens matched as the analyzer produces them, with no index built.</li>
 *   <li>{@link #readOnly} — the value's bytes read for the query's terms, analyzing nothing.</li>
 *   <li>{@code readThen*} — the read in front of each, which is how a query answers a field's values.</li>
 * </ul>
 *
 * <p>{@code holding} decides what the read can do: where no value spells the query's terms it rules every one of
 * them out, and where all of them do it is paid for nothing.
 *
 * <p>The values are generated text, unless {@code -DtextValueMatching.corpus=<file>} names a file of real ones,
 * one a line, which is cut into pieces of {@code valueLength} bytes at the spaces between its words. A corpus
 * decides for itself which values spell the query's terms, so {@code holding} picks terms that much of it
 * spells rather than planting them, and {@link #setup} reports what it found.
 *
 * <p>{@link #setup} checks that every way answers the same values before any of them is measured, so one that
 * matched too few fails the benchmark rather than winning it.
 *
 * <pre>{@code
 * ./gradlew -p benchmarks run --args 'TextValueMatchingBenchmark -jvmArgsAppend "--add-modules=jdk.incubator.vector"'
 * }</pre>
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TextValueMatchingBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    /** Bytes of text one invocation matches, which makes the reported throughput MiB/s. */
    static final int TEXT_PER_INVOCATION = 1 << 20;

    private static final String FIELD = "body";
    private static final String[] VOCABULARY = {
        "the",
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "lazy",
        "dog",
        "while",
        "nothing",
        "else",
        "happens" };
    /** A term the generated values never spell, so a value holds it only where it is put there on purpose. */
    private static final String ABSENT = "qqzzx";

    /** A file of values, one a line, measured instead of generated text. */
    private static final String CORPUS = System.getProperty("textValueMatching.corpus");
    /** Terms of English text, by how much of a corpus of it spells them. */
    private static final String[] FEW_VALUES_SPELL = { "music", "river", "government" };
    private static final String[] MOST_VALUES_SPELL = { "the", "of", "and" };

    /** The corpus cut into pieces, by the length of a piece, read once however many threads measure it. */
    private static final Map<Integer, List<BytesRef>> PIECES = new HashMap<>();

    @Param({ "NONE", "FEW", "ALL" })
    public String holding;

    @Param({ "512", "4096" })
    public int valueLength;

    @Param({ "1", "3" })
    public int queryTerms;

    private final Analyzer analyzer = new StandardAnalyzer();
    private final MemoryIndex reused = new MemoryIndex(true, false);

    private List<BytesRef> values;
    private Query query;
    private CandidateValues candidates;
    private TokenStreamMatching.Matcher matcher;

    @Setup
    public void setup() throws IOException {
        final Random random = new Random(42);
        final int count = TEXT_PER_INVOCATION / valueLength;
        values = CORPUS == null ? generated(random, count) : read(random, count);

        final Set<BytesRef> terms = new LinkedHashSet<>();
        final BooleanQuery.Builder anyTerm = new BooleanQuery.Builder();
        for (int term = 0; term < queryTerms; term++) {
            final String spelling = spelling(term);
            terms.add(new BytesRef(spelling));
            anyTerm.add(new TermQuery(new Term(FIELD, spelling)), Occur.SHOULD);
        }
        query = queryTerms == 1 ? new TermQuery(new Term(FIELD, spelling(0))) : anyTerm.build();
        matcher = new TokenStreamMatching.AnyTerm(terms);
        candidates = CandidateValues.of(query, FIELD).readingTokensOf(analyzer);
        if (candidates.reads() == false) {
            throw new AssertionError("the query names no term its values can be read for");
        }

        final int indexing = memoryIndex();
        for (String what : new String[] { "tokenStream", "readThenMemoryIndex", "readThenTokenStream" }) {
            final int answered = switch (what) {
                case "tokenStream" -> tokenStream();
                case "readThenMemoryIndex" -> readThenMemoryIndex();
                default -> readThenTokenStream();
            };
            if (answered != indexing) {
                throw new AssertionError(what + " answered " + answered + " values of " + indexing);
            }
        }
        if (CORPUS != null) {
            report(indexing);
        }
    }

    /** The values the generated text makes, as many of them spelling the query's term as {@link #holding} says. */
    private List<BytesRef> generated(Random random, int count) {
        final int holders = switch (holding) {
            case "NONE" -> 0;
            case "FEW" -> Math.max(1, count / 100);
            default -> count;
        };
        final List<BytesRef> generated = new ArrayList<>(count);
        for (int value = 0; value < count; value++) {
            generated.add(new BytesRef(value(random, value < holders)));
        }
        return generated;
    }

    /** {@code count} values of the corpus, from a place in it this {@code random} picks. */
    private List<BytesRef> read(Random random, int count) throws IOException {
        final List<BytesRef> pieces = pieces(valueLength);
        final List<BytesRef> picked = new ArrayList<>(count);
        final int from = random.nextInt(pieces.size());
        for (int value = 0; value < count; value++) {
            picked.add(pieces.get((from + value) % pieces.size()));
        }
        return picked;
    }

    /** The corpus cut into pieces of at least {@code length} bytes, at the spaces between its words. */
    private static synchronized List<BytesRef> pieces(int length) throws IOException {
        final List<BytesRef> cached = PIECES.get(length);
        if (cached != null) {
            return cached;
        }
        final List<BytesRef> pieces = new ArrayList<>();
        try (BufferedReader lines = Files.newBufferedReader(Path.of(CORPUS), StandardCharsets.UTF_8)) {
            for (String line = lines.readLine(); line != null; line = lines.readLine()) {
                final byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                int from = 0;
                while (from < bytes.length) {
                    int to = Math.min(bytes.length, from + length);
                    while (to < bytes.length && bytes[to] != ' ') {
                        to++;  // A cut lands between two words, where it can never split a character.
                    }
                    pieces.add(new BytesRef(Arrays.copyOfRange(bytes, from, to)));
                    from = to + 1;
                }
            }
        }
        if (pieces.isEmpty()) {
            throw new AssertionError(CORPUS + " holds no values");
        }
        PIECES.put(length, pieces);
        return pieces;
    }

    /** The {@code term}th term of the query, which {@link #holding} of the values spell. */
    private String spelling(int term) {
        if (CORPUS == null) {
            return term == 0 ? ABSENT : ABSENT + term;  // Which values spell it is decided when they are generated.
        }
        return switch (holding) {
            case "NONE" -> term == 0 ? ABSENT : ABSENT + term;
            case "FEW" -> FEW_VALUES_SPELL[term % FEW_VALUES_SPELL.length];
            default -> MOST_VALUES_SPELL[term % MOST_VALUES_SPELL.length];
        };
    }

    /** What the corpus turned out to hold, which it decides and the parameters only ask for. */
    private void report(int answered) {
        long bytes = 0;
        long nonAscii = 0;
        int valuesWithNonAscii = 0;
        for (BytesRef value : values) {
            bytes += value.length;
            int in = 0;
            for (int at = value.offset; at < value.offset + value.length; at++) {
                if (value.bytes[at] < 0) {
                    in++;
                }
            }
            nonAscii += in;
            if (in > 0) {
                valuesWithNonAscii++;
            }
        }
        System.out.printf(
            "# corpus: %,d values of %,d bytes, %.1f%% answer the query; %.2f%% of the bytes are not ASCII, in %.1f%% of the values%n",
            values.size(),
            bytes,
            100.0 * answered / values.size(),
            100.0 * nonAscii / bytes,
            100.0 * valuesWithNonAscii / values.size()
        );
    }

    /** A value of about {@link #valueLength} bytes, spelling the query's term only where it is told to. */
    private String value(Random random, boolean spellsTheTerm) {
        final StringBuilder text = new StringBuilder(valueLength + 8);
        while (text.length() < valueLength) {
            text.append(text.isEmpty() ? "" : " ").append(VOCABULARY[random.nextInt(VOCABULARY.length)]);
        }
        if (spellsTheTerm) {
            // At its end, so a walk of the value reaches it last: the read's best case is not what is measured.
            text.append(' ').append(ABSENT);
        }
        return text.toString();
    }

    @Benchmark
    public int memoryIndex() {
        int answered = 0;
        for (BytesRef value : values) {
            if (indexAndSearch(value) > 0) {
                answered++;
            }
        }
        return answered;
    }

    @Benchmark
    public int tokenStream() throws IOException {
        int answered = 0;
        for (BytesRef value : values) {
            if (matchOnTokenStream(value)) {
                answered++;
            }
        }
        return answered;
    }

    @Benchmark
    public int readOnly() {
        int kept = 0;
        for (BytesRef value : values) {
            if (candidates.mayHold(value)) {
                kept++;
            }
        }
        return kept;
    }

    @Benchmark
    public int readThenMemoryIndex() {
        int answered = 0;
        for (BytesRef value : values) {
            if (candidates.mayHold(value) && indexAndSearch(value) > 0) {
                answered++;
            }
        }
        return answered;
    }

    /** The read that answers for itself where it can, analyzing only the values its bytes leave unsettled. */
    @Benchmark
    public int heldThenTokenStream() throws IOException {
        int answered = 0;
        for (BytesRef value : values) {
            switch (candidates.held(value)) {
                case YES -> answered++;
                case MAYBE -> {
                    if (matchOnTokenStream(value)) {
                        answered++;
                    }
                }
                case NO -> {
                }
            }
        }
        return answered;
    }

    @Benchmark
    public int readThenTokenStream() throws IOException {
        int answered = 0;
        for (BytesRef value : values) {
            if (candidates.mayHold(value) && matchOnTokenStream(value)) {
                answered++;
            }
        }
        return answered;
    }

    private float indexAndSearch(BytesRef value) {
        reused.reset();
        reused.addField(FIELD, value.utf8ToString(), analyzer);
        return reused.search(query);
    }

    private boolean matchOnTokenStream(BytesRef value) throws IOException {
        try (TokenStream stream = analyzer.tokenStream(FIELD, value.utf8ToString())) {
            stream.reset();
            final boolean matched = matcher.matches(stream);
            stream.end();
            return matched;
        }
    }
}
