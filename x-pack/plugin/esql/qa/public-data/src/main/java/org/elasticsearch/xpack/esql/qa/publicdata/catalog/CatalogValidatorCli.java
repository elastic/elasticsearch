/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Offline catalog + spec validation entry point for the {@code validatePublicDataCatalog} Gradle
 * task (the unit tests call {@link CatalogValidator} directly). Also enforces the no-data-files
 * rule: nothing under {@code src/} may be a data or SQL file — expected tables live inside the
 * csv-specs, data bytes never leave the object store, and oracle outputs are never checked in.
 */
public final class CatalogValidatorCli {

    /** File suffixes that must never appear under src/: data bytes, oracle scripts and outputs. */
    private static final List<String> FORBIDDEN_SUFFIXES = List.of(
        ".sql",
        ".parquet",
        ".orc",
        ".gz",
        ".gzip",
        ".zst",
        ".zstd",
        ".snappy",
        ".csv",
        ".tsv",
        ".ndjson",
        ".jsonl"
    );

    private CatalogValidatorCli() {}

    @SuppressForbidden(reason = "CLI tool reports to stdout/stderr and sets an exit code")
    public static void main(String[] args) throws IOException {
        Path projectDir = Path.of(argValue(args, "--project-dir", "."));
        Path resourcesDir = projectDir.resolve("src/main/resources");
        Path catalogFile = resourcesDir.resolve("public-data-catalog.yml");
        if (Files.exists(catalogFile) == false) {
            System.err.println("Catalog file not found: " + catalogFile.toAbsolutePath());
            System.exit(2);
        }

        List<String> errors = new ArrayList<>();
        PublicDataCatalog catalog = null;
        try {
            catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        } catch (RuntimeException e) {
            errors.add("catalog failed to parse: " + e.getMessage());
        }
        if (catalog != null) {
            errors.addAll(CatalogValidator.validate(catalog, loadWorkloads(resourcesDir)));
        }
        errors.addAll(scanForForbiddenFiles(projectDir.resolve("src")));

        if (errors.isEmpty()) {
            System.out.println("public-data catalog and specs are structurally valid");
            return;
        }
        System.err.println("public-data catalog validation failed with " + errors.size() + " error(s):");
        for (String error : errors) {
            System.err.println("  - " + error);
        }
        System.exit(1);
    }

    private static Map<String, WorkloadSpec> loadWorkloads(Path resourcesDir) throws IOException {
        Map<String, WorkloadSpec> workloads = new LinkedHashMap<>();
        try (Stream<Path> files = Files.list(resourcesDir)) {
            files.filter(f -> {
                String name = f.getFileName().toString();
                return name.startsWith("public-") && name.endsWith(".csv-spec");
            }).sorted().forEach(f -> {
                try {
                    workloads.put(f.getFileName().toString(), WorkloadSpec.parse(f.getFileName().toString(), Files.readAllLines(f)));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        return workloads;
    }

    private static List<String> scanForForbiddenFiles(Path srcDir) throws IOException {
        List<String> errors = new ArrayList<>();
        if (Files.isDirectory(srcDir) == false) {
            return errors;
        }
        try (Stream<Path> files = Files.walk(srcDir)) {
            files.filter(Files::isRegularFile).forEach(f -> {
                String name = f.getFileName().toString().toLowerCase(Locale.ROOT);
                if (name.endsWith(".csv-spec")) {
                    return;
                }
                for (String suffix : FORBIDDEN_SUFFIXES) {
                    if (name.endsWith(suffix)) {
                        errors.add("forbidden data/SQL file under src/: " + f);
                    }
                }
            });
        }
        return errors;
    }

    private static String argValue(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return fallback;
    }
}
