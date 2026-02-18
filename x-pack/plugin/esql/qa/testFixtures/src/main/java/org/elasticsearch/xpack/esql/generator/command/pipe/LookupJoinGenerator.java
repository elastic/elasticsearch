/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.LookupIdx;
import org.elasticsearch.xpack.esql.generator.LookupIdxColumn;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;

public class LookupJoinGenerator implements CommandGenerator {

    public static final String LOOKUP_JOIN = "lookup join";
    public static final CommandGenerator INSTANCE = new LookupJoinGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        LookupIdx lookupIdx = randomFrom(schema.lookupIndices());
        String lookupIdxName = lookupIdx.idxName();
        int joinColumnsCount = randomInt(lookupIdx.keys().size() - 1) + 1; // at least one column must be used for the join
        List<LookupIdxColumn> joinColumns = ESTestCase.randomSubsetOf(joinColumnsCount, lookupIdx.keys());
        List<String> keyNames = new ArrayList<>();
        List<String> joinOn = new ArrayList<>();
        Set<String> usedColumns = new HashSet<>();
        for (LookupIdxColumn joinColumn : joinColumns) {
            String idxKey = joinColumn.name();
            String keyType = joinColumn.type();

            var candidateKeys = previousOutput.stream().filter(x -> x.type().equals(keyType)).toList();
            if (candidateKeys.isEmpty()) {
                continue; // no candidate keys of the right type, skip this column
            }
            Column key = randomFrom(candidateKeys);
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
            String keyName = keyNames.get(i);
            String joinName = joinOn.get(i);
            if (EsqlQueryGenerator.needsQuoting(keyName)) {
                keyName = EsqlQueryGenerator.quote(keyName);
            }
            if (EsqlQueryGenerator.needsQuoting(joinName)) {
                joinName = EsqlQueryGenerator.quote(joinName);
            }
            stringBuilder.append("| rename ");
            stringBuilder.append(keyName);
            stringBuilder.append(" as ");
            stringBuilder.append(joinName);
        }
        stringBuilder.append(" | lookup join ").append(lookupIdxName).append(" on ");
        for (int i = 0; i < keyNames.size(); i++) {
            String joinName = joinOn.get(i);
            if (EsqlQueryGenerator.needsQuoting(joinName)) {
                joinName = EsqlQueryGenerator.quote(joinName);
            }
            stringBuilder.append(joinName);
            if (i < keyNames.size() - 1) {
                stringBuilder.append(", ");
            }
        }
        String cmdString = stringBuilder.toString();
        return new CommandDescription(LOOKUP_JOIN, this, cmdString, Map.of("nKeys", keyNames.size()));
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

        // this is for the additional RENAME, that could drop columns
        int prevCols = previousColumns.size() - (Integer) commandDescription.context().get("nKeys");

        if (previousColumns.stream().anyMatch(x -> x.name().equals("<all-fields-projected>"))) {
            // known bug https://github.com/elastic/elasticsearch/issues/121741
            prevCols--;
        }

        // todo: awaits fix https://github.com/elastic/elasticsearch/issues/142636
        // if (prevCols > columns.size()) {
        // return new ValidationResult(false, "Expecting at least [" + prevCols + "] columns, got [" + columns.size() + "]");
        // }

        return VALIDATION_OK;
    }
}
