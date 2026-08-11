/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.SpecReader;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Regenerates {@code build/reports/public-data-coverage.md}: a per-suite dimension-coverage report over
 * every {@link PublicDataSource} in the catalog (plan section 7). Run manually via the
 * {@code publicDataCoverageReport} Gradle task; never part of any automated flow.
 */
public final class CoverageReportGenerator {

    private CoverageReportGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: CoverageReportGenerator <output-reports-dir>");
        }
        // This is a plain JavaExec main(), not an ESTestCase-derived JUnit run, so nothing has bootstrapped
        // ES logging yet; SpecReader/EsqlTestUtils's static initializers call LogManager.getLogger(...),
        // which throws an NPE without this (see ESTestCase#setTestSysProps, the equivalent JUnit-side call).
        LogConfigurator.configureESLogging();
        Path reportsDir = Path.of(args[0]);
        Files.createDirectories(reportsDir);
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath();
        String report = generate(catalog);
        Path out = reportsDir.resolve("public-data-coverage.md");
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println("Wrote " + out);
    }

    public static String generate(PublicDataCatalog catalog) throws Exception {
        StringBuilder md = new StringBuilder();
        md.append("# Public-data ES|QL suite: dimension coverage\n\n");
        md.append(
            "Generated from `public-data-catalog.yml` and every checked-in csv-spec (elastic/esql-planning#1650). "
                + "Every row below is an already-public object the upstream publisher exposes today; "
                + "an empty cell in the aggregate matrix is a genuine coverage gap, never a synthesized variant.\n\n"
        );

        EnumSet<PublicDataFormat> allFormats = EnumSet.noneOf(PublicDataFormat.class);
        EnumSet<PublicDataCodec> allCodecs = EnumSet.noneOf(PublicDataCodec.class);
        EnumSet<PublicDataProvider> allProviders = EnumSet.noneOf(PublicDataProvider.class);
        EnumSet<PartitionLayout> allLayouts = EnumSet.noneOf(PartitionLayout.class);
        TreeSet<String> aggregate = new TreeSet<>();
        int notCrossValidated = 0;
        int totalVariants = 0;
        int totalQueries = 0;

        for (PublicDataSource source : catalog.sources()) {
            md.append("## ").append(source.displayName()).append(" (`").append(source.id()).append("`)\n\n");
            md.append("- Homepage: ").append(source.homepage()).append('\n');
            md.append("- License: ").append(source.license()).append('\n');
            md.append("- Query provenance: ").append(source.queryProvenance()).append('\n');

            Set<String> specResources = new LinkedHashSet<>();
            for (SourceVariant v : source.variants()) {
                specResources.add(v.specResource());
            }
            int queries = 0;
            for (String specResource : specResources) {
                URL specUrl = CoverageReportGenerator.class.getResource(specResource);
                queries += specUrl == null ? 0 : SpecReader.readScriptSpec(List.of(specUrl), CsvSpecReader::specParser).size();
            }
            totalQueries += queries;
            md.append("- Distinct spec files: ").append(specResources.size()).append('\n');
            md.append("- Queries (summed across those spec files): ").append(queries).append('\n');
            md.append("- Variants: ").append(source.variants().size()).append("\n\n");

            md.append("| variant | spec | format | codec | provider | layout | scale | cross-validated | notes |\n");
            md.append("|---|---|---|---|---|---|---|---|---|\n");
            for (SourceVariant v : source.variants()) {
                totalVariants++;
                allFormats.add(v.format());
                allCodecs.add(v.codec());
                allProviders.add(v.provider());
                allLayouts.add(v.partitionLayout());
                aggregate.add(v.format() + " x " + v.codec() + " x " + v.provider() + " x " + v.partitionLayout());
                if (v.crossValidated() == false) {
                    notCrossValidated++;
                }
                md.append("| ")
                    .append(v.id())
                    .append(" | ")
                    .append(v.specResource())
                    .append(" | ")
                    .append(v.format())
                    .append(" | ")
                    .append(v.codec())
                    .append(" | ")
                    .append(v.provider())
                    .append(" | ")
                    .append(v.partitionLayout())
                    .append(" | ")
                    .append(v.scale())
                    .append(" | ")
                    .append(v.crossValidated() ? "yes" : "**no**")
                    .append(" | ")
                    .append(v.notes().replace("|", "\\|"))
                    .append(" |\n");
            }
            md.append('\n');
        }

        md.append("## Aggregate matrix\n\n");
        md.append("- Total sources: ").append(catalog.sources().size()).append('\n');
        md.append("- Total variants: ").append(totalVariants).append('\n');
        md.append("- Total queries (pre-variant-expansion): ").append(totalQueries).append('\n');
        md.append("- Variants without an independent DuckDB/ClickHouse cross-check: ").append(notCrossValidated).append('\n');
        md.append("- Formats covered: ").append(allFormats).append('\n');
        md.append("- Codecs covered: ").append(allCodecs).append('\n');
        md.append("- Providers covered: ").append(allProviders).append('\n');
        md.append("- Partition layouts covered: ").append(allLayouts).append('\n');
        md.append("- Distinct format x codec x provider x layout combinations exercised: ").append(aggregate.size()).append('\n');

        EnumSet<PublicDataFormat> missingFormats = EnumSet.complementOf(allFormats);
        EnumSet<PublicDataCodec> missingCodecs = EnumSet.complementOf(allCodecs);
        EnumSet<PartitionLayout> missingLayouts = EnumSet.complementOf(allLayouts);
        if (missingFormats.isEmpty() == false) {
            md.append("- **Uncovered formats:** ").append(missingFormats).append('\n');
        }
        if (missingCodecs.isEmpty() == false) {
            md.append("- **Uncovered codecs:** ").append(missingCodecs).append('\n');
        }
        if (missingLayouts.isEmpty() == false) {
            md.append("- **Uncovered partition layouts:** ").append(missingLayouts).append('\n');
        }
        return md.toString();
    }
}
