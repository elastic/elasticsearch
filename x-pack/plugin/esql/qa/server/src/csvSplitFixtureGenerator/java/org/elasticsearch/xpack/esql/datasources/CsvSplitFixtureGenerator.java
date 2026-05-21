/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Build-time generator that splits a CSV fixture file into N parts, each retaining the
 * original header line. The split preserves raw line content exactly (no parsing/re-emission),
 * so multi-value bracket syntax and typed headers are kept intact.
 * <p>
 * Usage: {@code CsvSplitFixtureGenerator <source.csv> <output-dir> <num-parts>}
 * <p>
 * Output: {@code <output-dir>/<basename>_00.csv}, {@code <basename>_01.csv}, ...
 */
public final class CsvSplitFixtureGenerator {

    private CsvSplitFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.out/err and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: CsvSplitFixtureGenerator <source.csv> <output-dir> <num-parts>");
            System.exit(1);
        }
        Path sourcePath = Path.of(args[0]);
        Path outputDir = Path.of(args[1]);
        int numParts = Integer.parseInt(args[2]);
        if (Files.exists(sourcePath) == false) {
            throw new IOException("Source CSV not found: " + sourcePath);
        }
        if (numParts < 1) {
            throw new IllegalArgumentException("num-parts must be >= 1, got " + numParts);
        }

        List<String> allLines = Files.readAllLines(sourcePath, StandardCharsets.UTF_8);
        if (allLines.isEmpty()) {
            throw new IOException("Source CSV is empty: " + sourcePath);
        }

        String header = allLines.get(0);
        List<String> dataLines = allLines.subList(1, allLines.size());
        int total = dataLines.size();
        int partSize = (total + numParts - 1) / numParts;

        Files.createDirectories(outputDir);
        String baseName = sourcePath.getFileName().toString().replaceFirst("\\.csv$", "");

        for (int part = 0; part < numParts; part++) {
            int from = part * partSize;
            int to = Math.min(from + partSize, total);
            if (from >= total) {
                break;
            }
            String fileName = String.format(Locale.ROOT, "%s_%02d.csv", baseName, part);
            Path outputPath = outputDir.resolve(fileName);
            StringBuilder sb = new StringBuilder();
            sb.append(header).append('\n');
            for (int i = from; i < to; i++) {
                sb.append(dataLines.get(i)).append('\n');
            }
            Files.writeString(outputPath, sb.toString(), StandardCharsets.UTF_8);
            System.out.println("Generated CSV split fixture: " + outputPath + " (" + (to - from) + " rows)");
        }
    }
}
