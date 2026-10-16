/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.stateless;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.benchmark.stateless.ReadRoundsDirectory.Kind;
import org.elasticsearch.index.store.LuceneFilesExtensions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.function.ToLongFunction;

/**
 * Counts the cold reads of loading the synthetic {@code _source} of a batch, without and with doc-values prefetch, on the
 * indices {@link DocValuesLoaderBenchmark} builds, for read units of 4kb, 64kb and 1mb: rounds waited one after another and
 * units fetched, split by what a unit holds (presence, offsets, values). See {@link ReadRoundsDirectory}.
 *
 * <p>Cold covers the doc-values files but the metadata ones ({@link LuceneFilesExtensions#isMetadata}), which a stateless
 * node warms whole when a segment opens. {@code unread} counts units prefetched and not read, {@code missed} units read and
 * not prefetched; both should be zero.
 *
 * <pre>{@code ./gradlew -p benchmarks docValuesPrefetchReads}</pre>
 */
public class DocValuesPrefetchReadsReport {

    private static final long[] UNITS = { 4 << 10, 64 << 10, 1 << 20 };

    public static void main(String[] args) throws IOException {
        System.out.println(
            "format    density batch   unit | serial rounds (presence/offsets/values) | prefetch rounds"
                + " | units (presence/offsets/values) unread missed"
        );
        for (String format : List.of("es819", "es95", "columnar")) {
            for (double density : new double[] { 1.0, 0.3 }) {
                for (int batchSize : new int[] { 16, 256 }) {
                    report(format, density, batchSize);
                }
            }
        }
    }

    /** The doc-values files that are not metadata. */
    private static boolean isColdDocValues(String file) {
        LuceneFilesExtensions extension = LuceneFilesExtensions.fromFile(file);
        return extension != null
            && extension.isMetadata() == false
            && (extension == LuceneFilesExtensions.DVD
                || extension == LuceneFilesExtensions.CND
                || extension == LuceneFilesExtensions.CNA
                || extension == LuceneFilesExtensions.CNL);
    }

    /** What a read of a file holds when the reading code does not say: offsets for ColumNAR addressing and lengths. */
    private static Kind kindOfFile(String file) {
        return file.endsWith(".cna") || file.endsWith(".cnl") ? Kind.OFFSETS : Kind.VALUES;
    }

    /** {@code count} for each kind, as {@code (presence/offsets/values)}. */
    private static String byKind(ToLongFunction<Kind> count) {
        return "(" + count.applyAsLong(Kind.PRESENCE) + "/" + count.applyAsLong(Kind.OFFSETS) + "/" + count.applyAsLong(Kind.VALUES) + ")";
    }

    private static void report(String format, double density, int batchSize) throws IOException {
        DocValuesLoaderBenchmark bench = new DocValuesLoaderBenchmark();
        bench.format = format;
        bench.numericFields = 10;
        bench.keywordFields = 10;
        bench.keywordCardinality = 1000;
        bench.numDocs = 200_000;
        bench.segments = 1;
        bench.batchSize = batchSize;
        bench.density = density;
        bench.rangeSize = "";
        Path path = Path.of(System.getProperty("java.io.tmpdir"), "stateless-bench-index", bench.indexCacheKey());
        if (Files.exists(path.resolve("_built")) == false) {
            Files.createDirectories(path);
            try (Directory dir = FSDirectory.open(path); IndexWriter writer = new IndexWriter(dir, bench.indexWriterConfig())) {
                bench.buildIndex(writer);
                writer.commit();
            }
            Files.createFile(path.resolve("_built"));
        }
        bench.prepareQuery();
        for (long unit : UNITS) {
            try (
                ReadRoundsDirectory dir = new ReadRoundsDirectory(
                    FSDirectory.open(path),
                    DocValuesPrefetchReadsReport::isColdDocValues,
                    DocValuesPrefetchReadsReport::kindOfFile,
                    unit
                );
                var reader = DirectoryReader.open(dir)
            ) {
                IndexSearcher searcher = new IndexSearcher(reader);
                bench.prefetchBudget = 0;
                dir.start();
                bench.runQuery(searcher);
                long serialRounds = dir.rounds();
                long serialUnits = dir.units();
                String serialRoundsByKind = byKind(dir::rounds);
                String unitsByKind = byKind(dir::units);
                bench.prefetchBudget = 1024;
                bench.prefetchReadUnit = (int) unit;
                dir.start();
                bench.runQuery(searcher);
                if (dir.units() != serialUnits) {
                    throw new AssertionError("prefetch read " + dir.units() + " units, a serial read " + serialUnits);
                }
                System.out.println(
                    String.format(
                        Locale.ROOT,
                        "%-9s %7.1f %5d %6s | %13d %-25s | %15d | %5d %-25s %6d %6d",
                        format,
                        density,
                        batchSize,
                        (unit >> 10) + "kb",
                        serialRounds,
                        serialRoundsByKind,
                        dir.rounds(),
                        serialUnits,
                        unitsByKind,
                        dir.unreadPrefetched(),
                        dir.unprefetchedReads()
                    )
                );
            }
        }
    }
}
