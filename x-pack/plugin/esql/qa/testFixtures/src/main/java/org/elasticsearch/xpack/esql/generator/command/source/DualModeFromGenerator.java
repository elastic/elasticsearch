/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;

/**
 * Source command generator for cross-index-mode differential tests.
 *
 * <p>Generates a {@code FROM <refPrefix>…} command for the reference side and records the
 * corresponding {@code FROM <candPrefix>…} command in the context under {@link #MIRROR_COMMAND},
 * so that the caller can execute the same pipeline against both index sets and compare results.
 *
 * <p>The wildcard patterns produced by {@link #indexPattern} are always namespace-scoped: the
 * reference prefix is preserved in every generated pattern (e.g. {@code ref_emp*}, never bare
 * {@code *}), which prevents cross-contamination between the two index sets regardless of how
 * aggressively {@link EsqlQueryGenerator#indexPattern} shortens the base name.
 *
 * <p>Unlike {@link FromGenerator}, this generator never emits {@code SET approximation=…}
 * (which samples, making results non-comparable across physically different indices). Subqueries
 * are also suppressed for the first cut: they require extra namespace-aware handling that is out
 * of scope here.
 */
public class DualModeFromGenerator extends FromGenerator {

    /**
     * Context key holding the candidate-side mirror of the generated source command string.
     * Set on the {@link CommandDescription} context by {@link #generate} so that the
     * cross-mode executor can build the candidate query without re-running the generator.
     */
    public static final String MIRROR_COMMAND = "mirrorCommand";

    private final String refPrefix;
    private final String candPrefix;

    public DualModeFromGenerator(String refPrefix, String candPrefix) {
        this.refPrefix = refPrefix;
        this.candPrefix = candPrefix;
    }

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        // SET prefixes are only legal at the top level, not inside a subquery.
        boolean useUnmappedFields = context.isWithinASubquery() == false && shouldAddUnmappedFieldWithProbabilityIncrease(3);
        StringBuilder result = new StringBuilder();
        if (useUnmappedFields) {
            result.append(SET_UNMAPPED_FIELDS_PREFIX);
        }
        // Deliberately omit SET approximation=… — approximation samples, so results are not
        // comparable across two physically different indices.
        Map<String, Object> commandContext = new HashMap<>();
        commandContext.put(UNMAPPED_FIELDS_ENABLED, useUnmappedFields);
        // Subqueries are suppressed: the SUBQUERIES GenerativeFeature is not enabled for the
        // cross-mode baseline, so canHaveSubquery in appendFromCommand will already be false.
        appendFromCommand(result, schema, executor, context, commandContext);
        String refCommand = result.toString();
        // Derive the candidate command by replacing refPrefix with candPrefix only at the start
        // of each index token (preceded by a comma or whitespace). This avoids incorrectly
        // replacing a refPrefix that appears as a substring within a future dataset name
        // (e.g. a dataset named "ref_data" would create a ref index "ref_ref_data", and a blanket
        // replace would turn "ref_ref_data*" into "cand_cand_data*" instead of "cand_ref_data*").
        String candCommand = refCommand.replaceAll("(?<=[,\\s])" + Pattern.quote(refPrefix), candPrefix);
        commandContext.put(MIRROR_COMMAND, candCommand);
        return new CommandDescription("from", this, refCommand, commandContext);
    }

    /**
     * Returns an index pattern that is always scoped to the reference namespace.
     * Strips the reference prefix from {@code indexName}, lets the base generator choose the
     * wildcard shape, then re-prepends the prefix. For example, if the base generator returns
     * {@code emp*} for {@code employees}, this method returns {@code ref_emp*}.
     * Even if the base generator returns a bare {@code *} (possible when truncation reaches
     * length 0), the result is {@code ref_*} — which only matches reference-side indices.
     */
    @Override
    protected String indexPattern(String indexName) {
        String baseName = indexName.startsWith(refPrefix) ? indexName.substring(refPrefix.length()) : indexName;
        return refPrefix + EsqlQueryGenerator.indexPattern(baseName);
    }
}
