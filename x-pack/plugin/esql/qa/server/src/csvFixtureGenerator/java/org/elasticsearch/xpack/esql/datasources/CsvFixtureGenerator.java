/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasources.fixtures.CsvFixtureParser;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureMatrix;
import org.elasticsearch.xpack.esql.datasources.fixtures.HivePartitioner;
import org.elasticsearch.xpack.esql.datasources.fixtures.TextRowRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Re-lays-out a source CSV into a Hive-partitioned directory tree.
 * <p>
 * The other CSV layouts are plain copies -- the source data already IS CSV, so a fixture is the
 * file itself -- and the build expresses them as Sync tasks. Hive is the one CSV layout that
 * needs row-level work, because which rows go in which directory depends on a column's value.
 * <p>
 * Rows are copied as their original source lines rather than re-rendered from parsed values.
 * Re-rendering would mean re-deciding quoting, multi-value bracket syntax and numeric formatting
 * for data that is already in exactly the right form; copying the line keeps the partitioned
 * fixture byte-identical to the rows it came from.
 */
public final class CsvFixtureGenerator {

    private CsvFixtureGenerator() {}

    private static final Logger logger = LoggerFactory.getLogger(CsvFixtureGenerator.class);
    private static final String HIVE_BY_FLAG = "--hive-by";
    private static final String VECTOR_VARIANTS_FLAG = "--vector-variants";

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.err and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length == 5 && HIVE_BY_FLAG.equals(args[2])) {
            Path sourcePath = Path.of(args[0]);
            Path outputDir = Path.of(args[1]);
            String sourceColumn = args[3];
            String partitionColumn = args[4];
            if (Files.exists(sourcePath) == false) {
                throw new IOException("Source CSV not found: " + sourcePath);
            }
            generateHivePartitionedByColumn(sourcePath, outputDir, sourceColumn, partitionColumn);
        } else if (args.length == 4 && VECTOR_VARIANTS_FLAG.equals(args[2])) {
            generateVectorVariants(Path.of(args[0]), Path.of(args[1]), args[3]);
        } else {
            System.err.println(
                "Usage: CsvFixtureGenerator <source-csv-path> <output-dir> --hive-by <source-column> <partition-column-name>"
                    + " | CsvFixtureGenerator <data-dir> <output-root> --vector-variants <format>"
            );
            System.exit(1);
        }
    }

    /**
     * Writes one file tree per dialect variant the contract declares for this format.
     *
     * <p>Enumerates from the contract, not from the capability table: a row lands only once a suite can
     * select the cell, which is after these bytes exist. Keying the enumeration on rows would render
     * nothing and report success.
     *
     * <p>A dataset the declaration says cannot be carried in a dialect is skipped with that licence and
     * logged. Anything else the renderer refuses is a real defect and propagates -- the difference
     * between a decision and an oversight has to survive to the build log.
     */
    private static void generateVectorVariants(Path dataDir, Path outputRoot, String format) throws IOException {
        FixtureDimensions dimensions = FixtureDimensions.get();
        FixtureMatrix matrix = FixtureMatrix.get();
        Map<String, Map<String, String>> slugs = dimensions.dialectSlugs(format);
        if (slugs.isEmpty()) {
            logger.info("No dialect variants declared for format [{}]; nothing to render", format);
            return;
        }
        for (Map.Entry<String, Map<String, String>> slug : slugs.entrySet()) {
            Map<String, String> pinned = slug.getValue();
            TextRowRenderer.Dialect dialect = dialectOf(pinned.getOrDefault("text_mode", dimensions.defaultValue("text_mode", format)));
            boolean headerRow = "false".equals(pinned.get("header_row")) == false;
            TextRowRenderer renderer = new TextRowRenderer(',', dialect, headerRow);
            Path slugRoot = outputRoot.resolve("vector").resolve(slug.getKey()).resolve("standalone");
            Files.createDirectories(slugRoot);
            for (String dataset : matrix.datasetsFor(format)) {
                if (matrix.unrepresentableDialects(dataset).contains(dialect.name().toLowerCase(Locale.ROOT))) {
                    logger.info("Skipping [{}] in [{}]: declared unrepresentable in {}", dataset, slug.getKey(), dialect);
                    continue;
                }
                Path source = dataDir.resolve(dataset + ".csv");
                if (Files.exists(source) == false) {
                    throw new IOException("dataset [" + dataset + "] is declared for [" + format + "] but [" + source + "] is missing");
                }
                String rendered = renderer.render(CsvFixtureParser.parseCsvFile(source));
                Path target = slugRoot.resolve(dataset + "." + format);
                Files.writeString(target, rendered, StandardCharsets.UTF_8);
                logger.info("Generated variant {}", target);
            }
        }
    }

    private static TextRowRenderer.Dialect dialectOf(String textMode) {
        return switch (textMode) {
            case "quoted" -> TextRowRenderer.Dialect.QUOTED;
            case "escaped" -> TextRowRenderer.Dialect.ESCAPED;
            case "plain" -> TextRowRenderer.Dialect.PLAIN;
            default -> throw new IllegalArgumentException("unknown text_mode [" + textMode + "]");
        };
    }

    private static void generateHivePartitionedByColumn(Path sourcePath, Path outputDir, String sourceColumn, String partitionColumn)
        throws IOException {
        List<String> lines = Files.readAllLines(sourcePath, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IOException("Source CSV is empty: " + sourcePath);
        }
        String header = lines.get(0);
        List<String> dataLines = lines.subList(1, lines.size());

        CsvFixtureParser.CsvFixtureResult parsed = CsvFixtureParser.parseCsvFile(sourcePath);
        // Copying source lines only works while one parsed row corresponds to one physical line.
        // A quoted embedded newline would break that correspondence silently, putting the wrong
        // rows in a partition, so refuse rather than guess.
        if (parsed.rows().size() != dataLines.size()) {
            throw new IOException(
                "Cannot partition ["
                    + sourcePath
                    + "] by copying source lines: it parses to "
                    + parsed.rows().size()
                    + " rows but has "
                    + dataLines.size()
                    + " data lines, so a row must span more than one line."
            );
        }

        // Identity, not equality: two rows with equal contents are still distinct rows, and each
        // one has its own source line.
        Map<Object[], String> lineByRow = new IdentityHashMap<>();
        for (int i = 0; i < parsed.rows().size(); i++) {
            lineByRow.put(parsed.rows().get(i), dataLines.get(i));
        }

        String baseName = sourcePath.getFileName().toString().replaceFirst("\\.csv$", "");
        Files.createDirectories(outputDir);
        for (Map.Entry<String, List<Object[]>> bucket : HivePartitioner.bucketRows(parsed, sourceColumn).entrySet()) {
            Path partitionDir = outputDir.resolve(HivePartitioner.partitionDirName(partitionColumn, bucket.getKey()));
            Files.createDirectories(partitionDir);
            Path outputPath = partitionDir.resolve(baseName + ".csv");

            List<String> out = new ArrayList<>();
            out.add(header);
            for (Object[] row : bucket.getValue()) {
                out.add(lineByRow.get(row));
            }
            Files.write(outputPath, String.join("\n", out).concat("\n").getBytes(StandardCharsets.UTF_8));
            logger.info("Generated CSV Hive partition: {} ({} rows)", outputPath, bucket.getValue().size());
        }
    }
}
