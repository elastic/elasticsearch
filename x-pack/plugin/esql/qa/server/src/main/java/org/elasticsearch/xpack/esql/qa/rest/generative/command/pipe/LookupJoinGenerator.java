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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;

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
        int joinColumnsCount = randomInt(lookupIdx.keys().size() - 1) + 1; // at least one column must be used for the join
        List<GenerativeRestTest.LookupIdxColumn> joinColumns = randomSubsetOf(joinColumnsCount, lookupIdx.keys());
        List<String> keyNames = new ArrayList<>();
        List<String> joinOn = new ArrayList<>();
        Set<String> usedColumns = new HashSet<>();
        for (GenerativeRestTest.LookupIdxColumn joinColumn : joinColumns) {
            String idxKey = joinColumn.name();
            String keyType = joinColumn.type();

            var candidateKeys = previousOutput.stream().filter(x -> x.type().equals(keyType)).toList();
            if (candidateKeys.isEmpty()) {
                continue; // no candidate keys of the right type, skip this column
            }
            EsqlQueryGenerator.Column key = randomFrom(candidateKeys);
            if (usedColumns.contains(key.name()) || usedColumns.contains(idxKey)) {
                continue; // already used this column from the lookup index, or will discard the main index column by RENAME'ing below, skip
            } else {
                usedColumns.add(key.name());
                usedColumns.add(idxKey);
            }
            keyNames.add(key.name());
            joinOn.add(idxKey);
        }
        if (keyNames.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < keyNames.size(); i++) {
            stringBuilder.append("| rename ");
            stringBuilder.append(keyNames.get(i));
            stringBuilder.append(" as ");
            stringBuilder.append(joinOn.get(i));
        }
        stringBuilder.append(" | lookup join ").append(lookupIdxName).append(" on ");
        for (int i = 0; i < keyNames.size(); i++) {
            stringBuilder.append(joinOn.get(i));
            if (i < keyNames.size() - 1) {
                stringBuilder.append(", ");
            }
        }
        String cmdString = stringBuilder.toString();
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
