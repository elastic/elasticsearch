/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation.Mode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonEmptySubsetOf;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.HAS_UNION_ALL;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.SET_UNMAPPED_FIELDS_PREFIX;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.SUBQUERY_PROBABILITY;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.UNMAPPED_FIELDS_ENABLED;

public class EqlGenerator implements CommandGenerator {

    public static final EqlGenerator INSTANCE = new EqlGenerator();

    public static final String EQL = "eql";

    static final String BRANCH = "eqlBranch";

    private static final List<String> SYNTHETIC_COLUMNS = List.of(
        EqlRelation.SEQUENCE_COLUMN,
        EqlRelation.SEQUENCE_STAGE_COLUMN,
        EqlRelation.JOIN_KEYS_COLUMN
    );
    private static final String SOURCE = "_source";
    private static final List<String> METADATA_FIELDS = List.of("_index", "_id", SOURCE);

    private static final int SAMPLED_ROWS = 100;
    private static final int MAX_INDEX_ATTEMPTS = 3;
    private static final int MAX_CONDITION_DEPTH = 2;
    private static final double SECOND_BRANCH_PROBABILITY = 0.3;
    private static final double METADATA_PROBABILITY = 0.3;

    private static final Set<String> DATE_TYPES = Set.of("date", "datetime", "date_nanos");
    private static final Set<String> KEY_TYPES = Set.of("keyword", "long", "integer", "ip");
    private static final Set<String> TIEBREAKER_TYPES = Set.of("long", "integer");
    private static final Set<String> NUMERIC_TYPES = Set.of("long", "integer", "double");
    private static final Set<String> CONVERTIBLE_TYPES = EqlRelation.CONVERTIBLE_TYPES.stream()
        .map(DataType::outputType)
        .collect(Collectors.toUnmodifiableSet());

    private static final Pattern EQL_FIELD_NAME = Pattern.compile("[A-Za-z_@][A-Za-z0-9_]*(?:\\.[A-Za-z_@][A-Za-z0-9_]*)*");
    private static final Set<String> EQL_KEYWORDS = Set.of(
        "and",
        "any",
        "by",
        "false",
        "in",
        "join",
        "like",
        "maxspan",
        "not",
        "null",
        "of",
        "or",
        "regex",
        "sample",
        "sequence",
        "true",
        "until",
        "where",
        "with"
    );

    record Branch(Mode mode, int stages, Integer maxMatches, List<String> metadata) {}

    private record IndexSample(
        String index,
        List<Column> columns,
        List<List<Object>> rows,
        Set<String> multiValued,
        Set<String> nullable
    ) {}

    private record Fields(
        List<String> timestamps,
        List<String> sequenceTimestamps,
        List<String> keys,
        List<String> tiebreakers,
        Map<String, List<String>> keywords,
        List<String> numerics,
        List<String> booleans,
        List<String> ips,
        boolean hasSyntheticName,
        boolean allConvertible
    ) {}

    private record BranchQuery(String text, Branch branch) {}

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        if (EsqlCapabilities.Cap.EQL_COMMAND.isEnabled()) {
            List<String> indices = schema.baseIndices();
            for (String index : randomSubsetOf(Math.min(MAX_INDEX_ATTEMPTS, indices.size()), indices)) {
                IndexSample sample = sampleIndex(index, executor);
                CommandDescription description = sample == null ? null : generateFromSample(sample, context);
                if (description != null) {
                    return description;
                }
            }
        }
        return FromGenerator.INSTANCE.generate(previousCommands, previousOutput, schema, executor, context);
    }

    private static IndexSample sampleIndex(String index, QueryExecutor executor) {
        QueryExecuted sampled = executor.execute("FROM " + index + " METADATA " + SOURCE + " | LIMIT " + SAMPLED_ROWS, 0);
        if (sampled.exception() != null || sampled.outputSchema() == null || sampled.result() == null) {
            return null;
        }
        List<Column> singleValueCandidates = new ArrayList<>();
        for (Column column : sampled.outputSchema()) {
            if (isEqlFieldName(column.name()) && (DATE_TYPES.contains(column.type()) || KEY_TYPES.contains(column.type()))) {
                singleValueCandidates.add(column);
            }
        }
        Set<String> multiValued = new HashSet<>();
        Set<String> nullable = new HashSet<>();
        if (singleValueCandidates.isEmpty() == false) {
            StringBuilder stats = new StringBuilder("FROM ").append(index).append(" | STATS total = COUNT(*)");
            for (int i = 0; i < singleValueCandidates.size(); i++) {
                String field = EsqlQueryGenerator.quote(singleValueCandidates.get(i).name());
                stats.append(", m").append(i).append(" = MAX(MV_COUNT(").append(field).append("))");
                stats.append(", c").append(i).append(" = COUNT(").append(field).append(")");
            }
            QueryExecuted counts = executor.execute(stats.toString(), 0);
            if (counts.exception() != null || counts.result() == null || counts.result().size() != 1) {
                return null;
            }
            List<Object> values = counts.result().getFirst();
            for (int i = 0; i < singleValueCandidates.size(); i++) {
                String name = singleValueCandidates.get(i).name();
                if (values.get(1 + 2 * i) instanceof Number maxCount && maxCount.intValue() > 1) {
                    multiValued.add(name);
                }
                if (values.getFirst() instanceof Number total
                    && values.get(2 + 2 * i) instanceof Number count
                    && count.longValue() == total.longValue()) {
                    continue;
                }
                nullable.add(name);
            }
        }
        return new IndexSample(index, sampled.outputSchema(), sampled.result(), multiValued, nullable);
    }

    private CommandDescription generateFromSample(IndexSample sample, GenerationContext context) {
        Fields fields = classify(sample);
        BranchQuery first = branch(sample, fields);
        if (first == null) {
            return null;
        }
        boolean topLevel = context.isWithinASubquery() == false;
        boolean useUnmappedFields = topLevel
            && EsqlCapabilities.Cap.EQL_COMMAND_UNMAPPED_FIELDS.isEnabled()
            && shouldAddUnmappedFieldWithProbabilityIncrease(3);
        boolean inSubquery = topLevel && EsqlCapabilities.Cap.EQL_IN_SUBQUERY.isEnabled() && randomDouble() < SUBQUERY_PROBABILITY;

        StringBuilder query = new StringBuilder();
        if (useUnmappedFields) {
            query.append(SET_UNMAPPED_FIELDS_PREFIX);
        }
        boolean union = false;
        if (inSubquery) {
            query.append("FROM ");
            // a FROM branch next to an EQL branch over the same index conflicts on the types EQL cannot convert yet
            // https://github.com/elastic/elasticsearch/issues/165569
            if (fields.allConvertible() && randomDouble() < SECOND_BRANCH_PROBABILITY) {
                query.append(sample.index()).append(", ");
                union = true;
            }
            query.append("(").append(first.text()).append(")");
            if (union == false && randomDouble() < SECOND_BRANCH_PROBABILITY) {
                BranchQuery second = branch(sample, fields);
                if (second != null) {
                    query.append(", (").append(second.text()).append(")");
                    union = true;
                }
            }
        } else {
            query.append(first.text());
        }

        Map<String, Object> commandContext = Map.of(
            UNMAPPED_FIELDS_ENABLED,
            useUnmappedFields,
            HAS_UNION_ALL,
            union,
            BRANCH,
            first.branch()
        );
        return new CommandDescription(EQL, this, query.toString(), commandContext);
    }

    private static Fields classify(IndexSample sample) {
        List<String> timestamps = new ArrayList<>();
        List<String> sequenceTimestamps = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        List<String> tiebreakers = new ArrayList<>();
        Map<String, List<String>> keywords = new LinkedHashMap<>();
        List<String> numerics = new ArrayList<>();
        List<String> booleans = new ArrayList<>();
        List<String> ips = new ArrayList<>();
        boolean hasSyntheticName = false;
        boolean allConvertible = true;
        int sourceIndex = columnIndex(sample, SOURCE);
        for (int c = 0; c < sample.columns().size(); c++) {
            Column column = sample.columns().get(c);
            String name = column.name();
            String type = column.type();
            if (c == sourceIndex) {
                continue;
            }
            hasSyntheticName |= SYNTHETIC_COLUMNS.contains(name);
            allConvertible &= CONVERTIBLE_TYPES.contains(type);
            if (isEqlFieldName(name) == false) {
                continue;
            }
            boolean singleValued = sample.multiValued().contains(name) == false;
            boolean inSource = singleValued && inSource(sample, c, sourceIndex);
            if (singleValued && DATE_TYPES.contains(type)) {
                timestamps.add(name);
                if (inSource) {
                    sequenceTimestamps.add(name);
                }
            }
            if (inSource && KEY_TYPES.contains(type)) {
                keys.add(name);
            }
            if (inSource && TIEBREAKER_TYPES.contains(type) && sample.nullable().contains(name) == false) {
                tiebreakers.add(name);
            }
            if (type.equals("keyword")) {
                keywords.put(name, stringValues(sample, c));
            }
            if (NUMERIC_TYPES.contains(type)) {
                numerics.add(name);
            }
            if (type.equals("boolean")) {
                booleans.add(name);
            }
            if (type.equals("ip")) {
                ips.add(name);
            }
        }
        return new Fields(
            timestamps,
            sequenceTimestamps,
            keys,
            tiebreakers,
            keywords,
            numerics,
            booleans,
            ips,
            hasSyntheticName,
            allConvertible
        );
    }

    private static int columnIndex(IndexSample sample, String name) {
        for (int i = 0; i < sample.columns().size(); i++) {
            if (sample.columns().get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean inSource(IndexSample sample, int column, int sourceIndex) {
        if (sourceIndex < 0) {
            return false;
        }
        String[] path = sample.columns().get(column).name().split("\\.");
        boolean witnessed = false;
        for (List<Object> row : sample.rows()) {
            if (row.get(column) == null) {
                continue;
            }
            if ((row.get(sourceIndex) instanceof Map<?, ?> source && containsPath(source, path, 0)) == false) {
                return false;
            }
            witnessed = true;
        }
        return witnessed;
    }

    private static boolean containsPath(Object node, String[] path, int from) {
        if (node instanceof List<?> list) {
            for (Object element : list) {
                if (containsPath(element, path, from)) {
                    return true;
                }
            }
            return false;
        }
        if (!(node instanceof Map<?, ?> map)) {
            return false;
        }
        StringBuilder key = new StringBuilder();
        for (int i = from; i < path.length; i++) {
            if (i > from) {
                key.append('.');
            }
            key.append(path[i]);
            String candidate = key.toString();
            if (map.containsKey(candidate) && (i == path.length - 1 || containsPath(map.get(candidate), path, i + 1))) {
                return true;
            }
        }
        return false;
    }

    private static List<String> stringValues(IndexSample sample, int column) {
        Set<String> values = new TreeSet<>();
        for (List<Object> row : sample.rows()) {
            if (row.get(column) instanceof String value) {
                values.add(value);
            }
        }
        return List.copyOf(values);
    }

    static boolean isEqlFieldName(String name) {
        if (EQL_FIELD_NAME.matcher(name).matches() == false) {
            return false;
        }
        for (String segment : name.split("\\.")) {
            if (EQL_KEYWORDS.contains(segment)) {
                return false;
            }
        }
        return true;
    }

    private static BranchQuery branch(IndexSample sample, Fields fields) {
        List<Mode> modes = new ArrayList<>(3);
        if (fields.timestamps().isEmpty() == false) {
            modes.add(Mode.EVENT);
        }
        if (fields.sequenceTimestamps().isEmpty() == false && fields.hasSyntheticName() == false) {
            modes.add(Mode.SEQUENCE);
        }
        if (fields.keys().isEmpty() == false && fields.hasSyntheticName() == false) {
            modes.add(Mode.SAMPLE);
        }
        if (modes.isEmpty()) {
            return null;
        }
        Mode mode = randomFrom(modes);

        Map<String, Object> options = new LinkedHashMap<>();
        if (mode != Mode.SAMPLE) {
            String timestamp = randomFrom(mode == Mode.SEQUENCE ? fields.sequenceTimestamps() : fields.timestamps());
            if (timestamp.equals("@timestamp") == false || randomBoolean()) {
                options.put("timestamp_field", timestamp);
            }
            if (fields.tiebreakers().isEmpty() == false && randomDouble() < 0.3) {
                options.put("tiebreaker_field", randomFrom(fields.tiebreakers()));
            }
        }
        String categoryField = fields.keywords().isEmpty() || randomBoolean() ? null : randomFrom(fields.keywords().keySet());
        if (categoryField != null && (categoryField.equals("event.category") == false || randomBoolean())) {
            options.put("event_category_field", categoryField);
        }
        Integer size = null;
        if (randomDouble() < 0.3) {
            size = randomIntBetween(0, 50);
            options.put("size", size);
        }
        if (randomDouble() < 0.15) {
            options.put("fetch_size", randomIntBetween(100, 1000));
        }
        if (mode == Mode.SAMPLE && randomDouble() < 0.3) {
            options.put("max_samples_per_key", randomIntBetween(1, 5));
        }
        if (randomDouble() < 0.2) {
            options.put("result_position", randomFrom("head", "tail"));
        }
        if (randomDouble() < 0.15) {
            options.put("allow_partial_sequence_results", randomBoolean());
        }

        StringBuilder eql = new StringBuilder();
        int stages = switch (mode) {
            case EVENT -> {
                eql.append(eventFilter(fields, categoryField));
                yield 1;
            }
            case SEQUENCE -> {
                int sequenceStages = randomIntBetween(2, 3);
                appendSequence(eql, fields, categoryField, sequenceStages);
                yield sequenceStages;
            }
            case SAMPLE -> {
                int sampleStages = randomIntBetween(2, 3);
                appendSample(eql, fields, categoryField, sampleStages);
                yield sampleStages;
            }
        };
        Integer maxMatches = size;
        if (mode != Mode.SAMPLE && randomDouble() < 0.2) {
            int limit = randomIntBetween(1, 20);
            eql.append(" | ").append(randomFrom("head", "tail")).append(" ").append(limit);
            maxMatches = maxMatches == null ? limit : Math.min(maxMatches, limit);
        }

        List<String> metadata = EsqlCapabilities.Cap.EQL_COMMAND_METADATA.isEnabled() && randomDouble() < METADATA_PROBABILITY
            ? randomNonEmptySubsetOf(METADATA_FIELDS)
            : List.of();

        StringBuilder text = new StringBuilder("EQL ").append(sample.index()).append(" ").append(quoteString(eql.toString()));
        if (metadata.isEmpty() == false) {
            text.append(" METADATA ").append(String.join(", ", metadata));
        }
        if (options.isEmpty() == false) {
            text.append(" WITH {");
            boolean firstOption = true;
            for (Map.Entry<String, Object> option : options.entrySet()) {
                if (firstOption == false) {
                    text.append(", ");
                }
                firstOption = false;
                text.append(quoteString(option.getKey())).append(": ");
                text.append(option.getValue() instanceof String s ? quoteString(s) : option.getValue());
            }
            text.append("}");
        }
        return new BranchQuery(text.toString(), new Branch(mode, stages, maxMatches, metadata));
    }

    private static void appendSequence(StringBuilder eql, Fields fields, String categoryField, int stages) {
        List<String> keys = randomSubsetOf(randomIntBetween(0, Math.min(2, fields.keys().size())), fields.keys());
        String stageKey = stageKey(fields, keys, false);
        eql.append("sequence");
        if (keys.isEmpty() == false) {
            eql.append(" by ").append(String.join(", ", keys));
        }
        if (randomDouble() < 0.3) {
            eql.append(" with maxspan=").append(randomIntBetween(1, 1000)).append(randomFrom("ms", "s", "m", "h", "d"));
        }
        for (int i = 0; i < stages; i++) {
            appendStage(eql, fields, categoryField, stageKey);
        }
        if (randomDouble() < 0.15) {
            eql.append(" until");
            appendStage(eql, fields, categoryField, stageKey);
        }
    }

    private static void appendSample(StringBuilder eql, Fields fields, String categoryField, int stages) {
        List<String> keys = randomSubsetOf(randomIntBetween(0, Math.min(2, fields.keys().size())), fields.keys());
        String stageKey = stageKey(fields, keys, keys.isEmpty());
        eql.append("sample");
        if (keys.isEmpty() == false) {
            eql.append(" by ").append(String.join(", ", keys));
        }
        for (int i = 0; i < stages; i++) {
            appendStage(eql, fields, categoryField, stageKey);
        }
    }

    private static String stageKey(Fields fields, List<String> keys, boolean required) {
        List<String> remaining = fields.keys().stream().filter(k -> keys.contains(k) == false).toList();
        if (remaining.isEmpty() || (required == false && randomBoolean())) {
            return null;
        }
        return randomFrom(remaining);
    }

    private static void appendStage(StringBuilder eql, Fields fields, String categoryField, String stageKey) {
        eql.append(" [").append(eventFilter(fields, categoryField)).append("]");
        if (stageKey != null) {
            eql.append(" by ").append(stageKey);
        }
    }

    private static String eventFilter(Fields fields, String categoryField) {
        String category = "any";
        if (categoryField != null && randomBoolean()) {
            category = quoteString(randomValue(fields.keywords().get(categoryField)));
        }
        return category + " where " + condition(fields, 0);
    }

    private static String condition(Fields fields, int depth) {
        if (depth < MAX_CONDITION_DEPTH && randomDouble() < 0.25) {
            return switch (randomIntBetween(0, 2)) {
                case 0 -> "(" + condition(fields, depth + 1) + ") and (" + condition(fields, depth + 1) + ")";
                case 1 -> "(" + condition(fields, depth + 1) + ") or (" + condition(fields, depth + 1) + ")";
                case 2 -> "not (" + condition(fields, depth + 1) + ")";
                default -> throw new IllegalStateException("unexpected logical operator");
            };
        }
        List<Supplier<String>> atoms = new ArrayList<>();
        atoms.add(() -> "true");
        if (fields.numerics().isEmpty() == false) {
            atoms.add(
                () -> randomFrom(fields.numerics()) + " " + randomFrom("==", "!=", "<", "<=", ">", ">=") + " " + randomIntBetween(-100, 100)
            );
        }
        if (fields.keywords().isEmpty() == false) {
            atoms.add(() -> keywordCondition(fields));
        }
        if (fields.booleans().isEmpty() == false) {
            atoms.add(() -> randomFrom(fields.booleans()) + " == " + randomBoolean());
        }
        if (fields.ips().isEmpty() == false) {
            atoms.add(
                () -> "cidrMatch("
                    + randomFrom(fields.ips())
                    + ", "
                    + quoteString(randomFrom("10.0.0.0/8", "192.168.0.0/16", "0.0.0.0/0"))
                    + ")"
            );
        }
        return randomFrom(atoms).get();
    }

    private static String keywordCondition(Fields fields) {
        String field = randomFrom(fields.keywords().keySet());
        String value = randomValue(fields.keywords().get(field));
        return switch (randomIntBetween(0, 5)) {
            case 0 -> field + " == " + quoteString(value);
            case 1 -> field + " : " + quoteString(value);
            case 2 -> "startsWith(" + field + ", " + quoteString(value.substring(0, randomIntBetween(0, value.length()))) + ")";
            case 3 -> "stringContains(" + field + ", " + quoteString(value.substring(randomIntBetween(0, value.length()))) + ")";
            case 4 -> "length(" + field + ") " + randomFrom("<", ">", "==") + " " + randomIntBetween(0, 20);
            case 5 -> field + randomFrom(" == null", " != null");
            default -> throw new IllegalStateException("unexpected keyword condition");
        };
    }

    private static String randomValue(List<String> values) {
        return values.isEmpty() ? randomAlphaOfLength(randomIntBetween(1, 10)) : randomFrom(values);
    }

    static String quoteString(String value) {
        StringBuilder quoted = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> quoted.append("\\\\");
                case '"' -> quoted.append("\\\"");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                case '\t' -> quoted.append("\\t");
                default -> quoted.append(c);
            }
        }
        return quoted.append('"').toString();
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
        if (commandDescription.generator() != this) {
            return commandDescription.generator()
                .validateOutput(previousCommands, commandDescription, previousColumns, previousOutput, columns, output);
        }
        if (Boolean.TRUE.equals(commandDescription.context().get(HAS_UNION_ALL))) {
            return VALIDATION_OK;
        }
        Branch branch = (Branch) commandDescription.context().get(BRANCH);
        List<String> names = columns.stream().map(Column::name).toList();
        for (String metadata : branch.metadata()) {
            if (names.contains(metadata) == false) {
                return new ValidationResult(false, "EQL METADATA column [" + metadata + "] is missing from " + names);
            }
        }
        if (branch.maxMatches() != null && output.size() > branch.maxMatches() * branch.stages()) {
            return new ValidationResult(
                false,
                "EQL returned ["
                    + output.size()
                    + "] rows for at most ["
                    + branch.maxMatches()
                    + "] matches of ["
                    + branch.stages()
                    + "] events"
            );
        }
        if (branch.mode() == Mode.EVENT) {
            return VALIDATION_OK;
        }
        if (names.containsAll(SYNTHETIC_COLUMNS) == false) {
            return new ValidationResult(
                false,
                "EQL " + branch.mode() + " query is missing synthetic columns " + SYNTHETIC_COLUMNS + " in " + names
            );
        }
        int sequenceIndex = names.indexOf(EqlRelation.SEQUENCE_COLUMN);
        int stageIndex = names.indexOf(EqlRelation.SEQUENCE_STAGE_COLUMN);
        for (List<Object> row : output) {
            if (!(row.get(sequenceIndex) instanceof Number sequence) || sequence.longValue() < 0) {
                return new ValidationResult(
                    false,
                    "EQL " + EqlRelation.SEQUENCE_COLUMN + " must be a non-negative number, got [" + row.get(sequenceIndex) + "]"
                );
            }
            if (!(row.get(stageIndex) instanceof Number stage) || stage.intValue() < 0 || stage.intValue() >= branch.stages()) {
                return new ValidationResult(
                    false,
                    "EQL "
                        + EqlRelation.SEQUENCE_STAGE_COLUMN
                        + " must be in [0, "
                        + branch.stages()
                        + "), got ["
                        + row.get(stageIndex)
                        + "]"
                );
            }
        }
        return VALIDATION_OK;
    }
}
