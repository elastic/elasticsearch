/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.oracle;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * AUTHORING AID ONLY, entry point of the {@code generateOracleScripts} Gradle task. Extracts each
 * shipped test's {@code // oracle-sql:} provenance into one runnable script per workload,
 * resolving {@code {{corpus}}} to the corpus's scratch download (or leaving in-place
 * {@code s3(..., NOSIGN)} reads untouched for small corpora and spot checks). Nothing in the suite
 * or the pipeline depends on this; it exists so regenerating expected results when the collection
 * grows never depends on remembering oracle incantations.
 *
 * <p>The scratch root is resolved from {@code $PUBLIC_DATA_SCRATCH} and defaults to
 * {@code ~/.cache/esql-public-data-scratch}; {@code {{corpus}}} resolves to
 * {@code file('<scratch>/<corpus-id>/...')} — a local copy downloaded ONCE per corpus. The
 * generated scripts and their outputs live under {@code build/} and are never checked in.
 */
public final class OracleScriptGenerator {

    private OracleScriptGenerator() {}

    @SuppressForbidden(reason = "CLI tool reports to stdout")
    public static void main(String[] args) throws IOException {
        Path outputDir = Path.of(argValue(args, "--output", "build/public-data-results/oracle"));
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        Oracle oracle = new ClickHouseLocalOracle();
        Files.createDirectories(outputDir);
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.workload() == null) {
                continue;
            }
            WorkloadSpec workload = WorkloadSpec.loadFromClasspath(corpus.workload());
            List<Oracle.OracleQuery> queries = new ArrayList<>();
            for (WorkloadSpec.TestSpec test : workload.tests()) {
                String sql = test.provenance().get("oracle-sql");
                if (sql != null && sql.isEmpty() == false) {
                    queries.add(new Oracle.OracleQuery(test.name(), sql));
                }
            }
            String scriptName = corpus.workload().replace(".csv-spec", ".sql");
            Path script = outputDir.resolve(scriptName);
            Files.writeString(script, oracle.renderScript(queries), StandardCharsets.UTF_8);
            System.out.println("wrote " + script + " (" + queries.size() + " queries; oracle: " + oracle.name() + ")");
        }
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
