/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.oracle;

import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * AUTHORING-TIME ONLY oracle SPI. An oracle establishes each expected answer once, while a corpus
 * is onboarded; what ships is the frozen, human-reviewed expected table plus its SQL as
 * provenance. The running suite has no oracle dependency, no oracle binary on the CI agent, and no
 * comparison leg — this package lives in its own source set precisely so nothing under
 * {@code src/main} or {@code src/publicDataTest} can reference it.
 */
public interface Oracle {

    /** Human-readable name and version, recorded as {@code // oracle:} provenance. */
    String name();

    /** Whether this oracle can read the variant's physical shape (seekability, format support). */
    boolean canRead(VariantSpec variant);

    /** Renders {@code queries} as a single executable script in this oracle's dialect/invocation. */
    String renderScript(List<OracleQuery> queries);

    /** Executes a rendered script and returns per-query outputs. */
    OracleResult run(Path script) throws IOException;

    /** One query to derive: the spec test it belongs to plus the SQL to run. */
    record OracleQuery(String testName, String sql) {}

    /** The outcome of a script run: raw stdout, stderr and the process exit code. */
    record OracleResult(int exitCode, String stdout, String stderr) {}
}
