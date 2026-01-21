/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.LookupIdx;
import org.elasticsearch.xpack.esql.generator.LookupIdxColumn;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.HashMap;
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
        List<String> joinOnLeft = new ArrayList<>();
        List<String> joinOnRight = new ArrayList<>();
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
            joinOnLeft.add(key.name());
            joinOnRight.add(idxKey);
        }
        if (joinOnLeft.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        StringBuilder stringBuilder = new StringBuilder();

        // Randomly choose between field-based join and expression-based join
        boolean useExpressionJoin = ESTestCase.randomBoolean();

        if (useExpressionJoin) {
            // Get all right side column names (from lookup index)
            Set<String> allRightColumnNames = lookupIdx.keys()
                .stream()
                .map(LookupIdxColumn::name)
                .collect(java.util.stream.Collectors.toSet());

            // Generate rename commands for ALL left columns that exist in right side
            Set<String> allLeftColumnNames = previousOutput.stream().map(Column::name).collect(java.util.stream.Collectors.toSet());
            Map<String, String> leftColumnName2Renamed = new HashMap<>();
            // Rename due to co1 == col2, the conflict is on col2 name
            for (String leftColumnName : allLeftColumnNames) {
                if (joinOnRight.contains(leftColumnName)) {
                    String renamedColumn = leftColumnName + "_left";
                    stringBuilder.append("| rename ");
                    stringBuilder.append(leftColumnName);
                    stringBuilder.append(" as ");
                    stringBuilder.append(renamedColumn);
                    leftColumnName2Renamed.put(leftColumnName, renamedColumn);
                }
            }

            // Rename due to co1 == col2, the conflict is on col1 name
            // but only rename if it wasn't renamed already
            for (String rightColumnName : allRightColumnNames) {
                if (joinOnLeft.contains(rightColumnName) && leftColumnName2Renamed.containsKey(rightColumnName) == false) {
                    String renamedColumn = rightColumnName + "_left";
                    stringBuilder.append("| rename ");
                    stringBuilder.append(rightColumnName);
                    stringBuilder.append(" as ");
                    stringBuilder.append(renamedColumn);
                    leftColumnName2Renamed.put(rightColumnName, renamedColumn);
                }
            }

            // Generate expression join syntax
            stringBuilder.append(" | lookup join ").append(lookupIdxName).append(" on ");

            // Add join conditions for all columns
            for (int i = 0; i < joinOnLeft.size(); i++) {
                String leftColumnName = joinOnLeft.get(i);
                String rightColumnName = joinOnRight.get(i);

                // Only == and != are allowed, as the rest of the operators don's support all types
                String[] booleanOperators = { "==", "!=" };
                String operator = randomFrom(booleanOperators);

                // Use renamed column if it was renamed, otherwise use original
                String finalLeftColumnName = leftColumnName2Renamed.getOrDefault(leftColumnName, leftColumnName);
                if (i > 0) {
                    stringBuilder.append(" AND ");
                }
                stringBuilder.append(finalLeftColumnName).append(" ").append(operator).append(" ").append(rightColumnName);
            }
        } else {
            // Generate field-based join (original behavior)
            for (int i = 0; i < joinOnLeft.size(); i++) {
                stringBuilder.append("| rename ");
                stringBuilder.append(joinOnLeft.get(i));
                stringBuilder.append(" as ");
                stringBuilder.append(joinOnRight.get(i));
            }
            stringBuilder.append(" | lookup join ").append(lookupIdxName).append(" on ");
            for (int i = 0; i < joinOnLeft.size(); i++) {
                stringBuilder.append(joinOnRight.get(i));
                if (i < joinOnLeft.size() - 1) {
                    stringBuilder.append(", ");
                }
            }
        }
        String cmdString = stringBuilder.toString();
        return new CommandDescription(LOOKUP_JOIN, this, cmdString, Map.of());
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
