/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.xpack.esql.CsvTestsDataLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

public class EsqlQueryGenerator {

    public record Column(String name, String type) {}

    public record QueryExecuted(String query, int depth, List<Column> outputSchema, Exception exception) {}

    public static String sourceCommand(List<String> availabeIndices) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> from(availabeIndices);
            // case 1 -> metaFunctions();
            default -> from(availabeIndices);
            // TODO re-enable ROW.
            // now it crashes nodes in some cases: exiting java.lang.AssertionError: estimated row size [0] wasn't set
            // default -> row();
        };

    }

    /**
     * @param previousOutput a list of fieldName+type
     * @param policies
     * @return a new command that can process it as input
     */
    public static String pipeCommand(
        List<Column> previousOutput,
        List<CsvTestsDataLoader.EnrichConfig> policies,
        List<GenerativeRestTest.LookupIdx> lookupIndices
    ) {
        return switch (randomIntBetween(0, 12)) {
            case 0 -> dissect(previousOutput);
            case 1 -> drop(previousOutput);
            case 2 -> enrich(previousOutput, policies);
            case 3 -> eval(previousOutput);
            case 4 -> grok(previousOutput);
            case 5 -> keep(previousOutput);
            case 6 -> limit();
            case 7 -> mvExpand(previousOutput);
            case 8 -> rename(previousOutput);
            case 9 -> sort(previousOutput);
            case 10 -> stats(previousOutput);
            case 11 -> join(previousOutput, lookupIndices);
            default -> where(previousOutput);
        };
    }

    private static String join(List<Column> previousOutput, List<GenerativeRestTest.LookupIdx> lookupIndices) {

        GenerativeRestTest.LookupIdx lookupIdx = randomFrom(lookupIndices);
        String lookupIdxName = lookupIdx.idxName();
        String idxKey = lookupIdx.key();
        String keyType = lookupIdx.keyType();

        var candidateKeys = previousOutput.stream().filter(x -> x.type.equals(keyType)).toList();
        if (candidateKeys.isEmpty()) {
            return "";
        }
        Column key = randomFrom(candidateKeys);
        return "| rename " + key.name + " as " + idxKey + " | lookup join " + lookupIdxName + " on " + idxKey;
    }

    private static String where(List<Column> previousOutput) {
        // TODO more complex conditions
        StringBuilder result = new StringBuilder(" | where ");
        int nConditions = randomIntBetween(1, 5);
        for (int i = 0; i < nConditions; i++) {
            String exp = booleanExpression(previousOutput);
            if (exp == null) {
                // cannot generate expressions, just skip
                return "";
            }
            if (i > 0) {
                result.append(randomBoolean() ? " AND " : " OR ");
            }
            if (randomBoolean()) {
                result.append(" NOT ");
            }
            result.append(exp);
        }

        return result.toString();
    }

    private static String booleanExpression(List<Column> previousOutput) {
        // TODO LIKE, RLIKE, functions etc.
        return switch (randomIntBetween(0, 3)) {
            case 0 -> {
                String field = randomNumericField(previousOutput);
                if (field == null) {
                    yield null;
                }
                yield field + " " + mathCompareOperator() + " 50";
            }
            case 1 -> "true";
            default -> "false";
        };
    }

    private static String mathCompareOperator() {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> "==";
            case 1 -> ">";
            case 2 -> ">=";
            case 3 -> "<";
            case 4 -> "<=";
            default -> "!=";
        };
    }

    private static String enrich(List<Column> previousOutput, List<CsvTestsDataLoader.EnrichConfig> policies) {
        String field = randomKeywordField(previousOutput);
        if (field == null || policies.isEmpty()) {
            return "";
        }

        // TODO add WITH
        return " | enrich " + randomFrom(policiesOnKeyword(policies)).policyName() + " on " + field;
    }

    private static List<CsvTestsDataLoader.EnrichConfig> policiesOnKeyword(List<CsvTestsDataLoader.EnrichConfig> policies) {
        // TODO make it smarter and extend it to other types
        return policies.stream().filter(x -> Set.of("languages_policy").contains(x.policyName())).toList();
    }

    private static String grok(List<Column> previousOutput) {
        String field = randomStringField(previousOutput);
        if (field == null) {
            return "";// no strings to grok, just skip
        }
        StringBuilder result = new StringBuilder(" | grok ");
        result.append(field);
        result.append(" \"");
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            if (i > 0) {
                result.append(" ");
            }
            result.append("%{WORD:");
            if (randomBoolean()) {
                result.append(randomIdentifier());
            } else {
                String fieldName = randomRawName(previousOutput);
                if (fieldName == null) {
                    fieldName = randomIdentifier();
                }
                result.append(fieldName);
            }
            result.append("}");
        }
        result.append("\"");
        return result.toString();
    }

    private static String dissect(List<Column> previousOutput) {
        String field = randomStringField(previousOutput);
        if (field == null) {
            return "";// no strings to dissect, just skip
        }
        StringBuilder result = new StringBuilder(" | dissect ");
        result.append(field);
        result.append(" \"");
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            if (i > 0) {
                result.append(" ");
            }
            result.append("%{");
            if (randomBoolean()) {
                result.append(randomIdentifier());
            } else {
                String fieldName = randomRawName(previousOutput);
                if (fieldName == null) {
                    fieldName = randomIdentifier();
                }
                result.append(fieldName);
            }
            result.append("}");
        }
        result.append("\"");
        return result.toString();
    }

    private static String keep(List<Column> previousOutput) {
        int n = randomIntBetween(1, previousOutput.size());
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            if (randomIntBetween(0, 100) < 5) {
                proj.add("*");
            } else {
                String name = randomName(previousOutput);
                if (name == null) {
                    continue;
                }
                if (name.length() > 1 && name.startsWith("`") == false && randomIntBetween(0, 100) < 10) {
                    if (randomBoolean()) {
                        name = name.substring(0, randomIntBetween(1, name.length() - 1)) + "*";
                    } else {
                        name = "*" + name.substring(randomIntBetween(1, name.length() - 1));
                    }
                }
                proj.add(name);
            }
        }
        if (proj.isEmpty()) {
            return "";
        }
        return " | keep " + proj.stream().collect(Collectors.joining(", "));
    }

    private static String randomName(List<Column> previousOutput) {
        String result = randomRawName(previousOutput);
        if (result == null) {
            return null;
        }
        if (randomBoolean() && result.contains("*") == false) {
            result = "`" + result + "`";
        }
        return result;
    }

    /**
     * Returns a field name from a list of columns.
     * Could be null if none of the fields can be considered
     */
    private static String randomRawName(List<Column> previousOutput) {
        var list = previousOutput.stream().filter(EsqlQueryGenerator::fieldCanBeUsed).toList();
        if (list.isEmpty()) {
            return null;
        }
        String result = randomFrom(list).name();
        return result;
    }

    /**
     * Returns a field that can be used for grouping.
     * Can return null
     */
    private static String randomGroupableName(List<Column> previousOutput) {
        var candidates = previousOutput.stream().filter(EsqlQueryGenerator::groupable).filter(EsqlQueryGenerator::fieldCanBeUsed).toList();
        if (candidates.isEmpty()) {
            return null;
        }
        return randomFrom(candidates).name();
    }

    private static boolean groupable(Column col) {
        return col.type.equals("keyword")
            || col.type.equals("text")
            || col.type.equals("long")
            || col.type.equals("integer")
            || col.type.equals("ip")
            || col.type.equals("version");
    }

    /**
     * returns a field that can be sorted.
     * Null if no fields are sortable.
     */
    private static String randomSortableName(List<Column> previousOutput) {
        var candidates = previousOutput.stream().filter(EsqlQueryGenerator::sortable).filter(EsqlQueryGenerator::fieldCanBeUsed).toList();
        if (candidates.isEmpty()) {
            return null;
        }
        return randomFrom(candidates).name();
    }

    private static boolean sortable(Column col) {
        return col.type.equals("keyword")
            || col.type.equals("text")
            || col.type.equals("long")
            || col.type.equals("integer")
            || col.type.equals("ip")
            || col.type.equals("version");
    }

    private static String rename(List<Column> previousOutput) {
        int n = randomIntBetween(1, Math.min(3, previousOutput.size()));
        List<String> proj = new ArrayList<>();

        Map<String, String> nameToType = new HashMap<>();
        for (Column column : previousOutput) {
            nameToType.put(column.name, column.type);
        }
        List<String> names = new ArrayList<>(
            previousOutput.stream().filter(EsqlQueryGenerator::fieldCanBeUsed).map(Column::name).collect(Collectors.toList())
        );
        if (names.isEmpty()) {
            return "";
        }
        for (int i = 0; i < n; i++) {
            if (names.isEmpty()) {
                break;
            }
            var name = randomFrom(names);
            if (nameToType.get(name).endsWith("_range")) {
                // ranges are not fully supported yet
                continue;
            }
            names.remove(name);

            String newName;
            if (names.isEmpty() || randomBoolean()) {
                newName = randomIdentifier();
                names.add(newName);
            } else {
                newName = names.get(randomIntBetween(0, names.size() - 1));
            }
            nameToType.put(newName, nameToType.get(name));
            if (randomBoolean() && name.startsWith("`") == false) {
                name = "`" + name + "`";
            }
            if (randomBoolean() && newName.startsWith("`") == false) {
                newName = "`" + newName + "`";
            }
            proj.add(name + " AS " + newName);
        }
        if (proj.isEmpty()) {
            return "";
        }
        return " | rename " + proj.stream().collect(Collectors.joining(", "));
    }

    private static String drop(List<Column> previousOutput) {
        if (previousOutput.size() < 2) {
            return ""; // don't drop all of them, just do nothing
        }
        int n = randomIntBetween(1, previousOutput.size() - 1);
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String name = randomRawName(previousOutput);
            if (name == null) {
                continue;
            }
            if (name.length() > 1 && name.startsWith("`") == false && randomIntBetween(0, 100) < 10) {
                if (randomBoolean()) {
                    name = name.substring(0, randomIntBetween(1, name.length() - 1)) + "*";
                } else {
                    name = "*" + name.substring(randomIntBetween(1, name.length() - 1));
                }
            } else if (name.startsWith("`") == false && (randomBoolean() || name.isEmpty())) {
                name = "`" + name + "`";
            }
            proj.add(name);
        }
        if (proj.isEmpty()) {
            return "";
        }
        return " | drop " + proj.stream().collect(Collectors.joining(", "));
    }

    private static String sort(List<Column> previousOutput) {
        int n = randomIntBetween(1, previousOutput.size());
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String col = randomSortableName(previousOutput);
            if (col == null) {
                return "";// no sortable columns
            }
            proj.add(col);
        }
        return " | sort "
            + proj.stream()
                .map(x -> x + randomFrom("", " ASC", " DESC") + randomFrom("", " NULLS FIRST", " NULLS LAST"))
                .collect(Collectors.joining(", "));
    }

    private static String mvExpand(List<Column> previousOutput) {
        String toExpand = randomName(previousOutput);
        if (toExpand == null) {
            return ""; // no columns to expand
        }
        return " | mv_expand " + toExpand;
    }

    private static String eval(List<Column> previousOutput) {
        StringBuilder cmd = new StringBuilder(" | eval ");
        int nFields = randomIntBetween(1, 10);
        // TODO pass newly created fields to next expressions
        for (int i = 0; i < nFields; i++) {
            String name;
            if (randomBoolean()) {
                name = randomIdentifier();
            } else {
                name = randomName(previousOutput);
                if (name == null) {
                    name = randomIdentifier();
                }
            }
            String expression = expression(previousOutput);
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            cmd.append(" = ");
            cmd.append(expression);
        }
        return cmd.toString();
    }

    private static String stats(List<Column> previousOutput) {
        List<Column> nonNull = previousOutput.stream()
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .filter(x -> x.type().equals("null") == false)
            .collect(Collectors.toList());
        if (nonNull.isEmpty()) {
            return ""; // cannot do any stats, just skip
        }
        StringBuilder cmd = new StringBuilder(" | stats ");
        int nStats = randomIntBetween(1, 5);
        for (int i = 0; i < nStats; i++) {
            String name;
            if (randomBoolean()) {
                name = randomIdentifier();
            } else {
                name = randomName(previousOutput);
                if (name == null) {
                    name = randomIdentifier();
                }
            }
            String expression = agg(nonNull);
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            cmd.append(" = ");
            cmd.append(expression);
        }
        if (randomBoolean()) {
            var col = randomGroupableName(nonNull);
            if (col != null) {
                cmd.append(" by " + col);
            }
        }
        return cmd.toString();
    }

    private static String agg(List<Column> previousOutput) {
        String name = randomNumericOrDateField(previousOutput);
        if (name != null && randomBoolean()) {
            // numerics only
            return switch (randomIntBetween(0, 1)) {
                case 0 -> "max(" + name + ")";
                default -> "min(" + name + ")";
                // TODO more numerics
            };
        }
        // all types
        name = randomName(previousOutput);
        if (name == null) {
            return "count(*)";
        }
        return switch (randomIntBetween(0, 2)) {
            case 0 -> "count(*)";
            case 1 -> "count(" + name + ")";
            default -> "count_distinct(" + name + ")";
        };
    }

    private static String randomNumericOrDateField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("long", "integer", "double", "date"));
    }

    private static String randomNumericField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("long", "integer", "double"));
    }

    private static String randomStringField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("text", "keyword"));
    }

    private static String randomKeywordField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("keyword"));
    }

    private static String randomName(List<Column> cols, Set<String> allowedTypes) {
        List<String> items = cols.stream().filter(x -> allowedTypes.contains(x.type())).map(Column::name).collect(Collectors.toList());
        if (items.size() == 0) {
            return null;
        }
        return items.get(randomIntBetween(0, items.size() - 1));
    }

    private static String expression(List<Column> previousOutput) {
        // TODO improve!!!
        return constantExpression();
    }

    public static String limit() {
        return " | limit " + randomIntBetween(0, 15000);
    }

    private static String from(List<String> availabeIndices) {
        StringBuilder result = new StringBuilder("from ");
        int items = randomIntBetween(1, 3);
        for (int i = 0; i < items; i++) {
            String pattern = indexPattern(availabeIndices.get(randomIntBetween(0, availabeIndices.size() - 1)));
            if (i > 0) {
                result.append(",");
            }
            result.append(pattern);
        }
        return result.toString();
    }

    private static String metaFunctions() {
        return "meta functions";
    }

    private static String indexPattern(String indexName) {
        return randomBoolean() ? indexName : indexName.substring(0, randomIntBetween(0, indexName.length())) + "*";
    }

    private static String row() {
        StringBuilder cmd = new StringBuilder("row ");
        int nFields = randomIntBetween(1, 10);
        for (int i = 0; i < nFields; i++) {
            String name = randomIdentifier();
            String expression = constantExpression();
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            cmd.append(" = ");
            cmd.append(expression);
        }
        return cmd.toString();
    }

    private static String constantExpression() {
        // TODO not only simple values, but also foldable expressions
        return switch (randomIntBetween(0, 4)) {
            case 0 -> "" + randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
            case 1 -> "" + randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            case 2 -> "\"" + randomAlphaOfLength(randomIntBetween(0, 20)) + "\"";
            case 3 -> "" + randomBoolean();
            default -> "null";
        };

    }

    private static String randomIdentifier() {
        // Let's create identifiers that are long enough to avoid collisions with reserved keywords.
        // There could be a smarter way (introspection on the lexer class?), but probably it's not worth the effort
        return randomAlphaOfLength(randomIntBetween(8, 12));
    }

    private static boolean fieldCanBeUsed(Column field) {
        return (
        // https://github.com/elastic/elasticsearch/issues/121741
        field.name().equals("<all-fields-projected>")
            // this is a known pathological case, no need to test it for now
            || field.name().equals("<no-fields>")) == false;
    }

}
