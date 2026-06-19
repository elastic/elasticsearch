/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;
import java.util.Map;

public class DedupGenerator implements CommandGenerator {

    public static final String DEDUP = "dedup";
    public static final CommandGenerator INSTANCE = new DedupGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        if (EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled() == false) {
            return EMPTY_DESCRIPTION;
        }
        // DEDUP runs over every column in scope, so any column with a type the runtime group-key
        // encoder can't handle would produce a VerificationException. Skip rather than emit a
        // query we know would be rejected. Must stay in sync with Aggregate.checkUnsupportedGroupingType.
        for (Column c : previousOutput) {
            DataType dt = DataType.fromTypeName(c.type());
            if (dt != null
                && (dt == DataType.AGGREGATE_METRIC_DOUBLE
                    || dt == DataType.DATE_RANGE
                    || dt == DataType.EXPONENTIAL_HISTOGRAM
                    || dt == DataType.TDIGEST
                    || dt == DataType.PARTIAL_AGG
                    || dt.isCounter())) {
                return EMPTY_DESCRIPTION;
            }
        }
        return new CommandDescription(DEDUP, this, " | DEDUP ", Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }

        return CommandGenerator.expectSameColumns(previousCommands, previousColumns, columns);
    }
}
