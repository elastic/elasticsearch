/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe;

import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomFrom;

public class LookupJoinGenerator implements CommandGenerator {

    public static final String LOOKUP_JOIN = "lookup join";
    public static final CommandGenerator INSTANCE = new LookupJoinGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        GenerativeRestTest.LookupIdx lookupIdx = randomFrom(schema.lookupIndices());
        String lookupIdxName = lookupIdx.idxName();
        String idxKey = lookupIdx.key();
        String keyType = lookupIdx.keyType();

        var candidateKeys = previousOutput.stream().filter(x -> x.type().equals(keyType)).toList();
        if (candidateKeys.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        EsqlQueryGenerator.Column key = randomFrom(candidateKeys);
        String cmdString = "| rename " + key.name() + " as " + idxKey + " | lookup join " + lookupIdxName + " on " + idxKey;
        return new CommandDescription(LOOKUP_JOIN, this, cmdString, Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    ) {
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }

        // the -1 is for the additional RENAME, that could drop one column
        int prevCols = previousColumns.size() - 1;

        if (previousColumns.stream().anyMatch(x -> x.name().equals("<all-fields-projected>"))) {
            // known bug https://github.com/elastic/elasticsearch/issues/121741
            prevCols--;
        }

        if (prevCols > columns.size()) {
            return new ValidationResult(false, "Expecting at least [" + prevCols + "] columns, got [" + columns.size() + "]");
        }
        return VALIDATION_OK;
    }
}
