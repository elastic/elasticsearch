/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.apache.http.HttpEntity;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.generator.AllowedGeneratorFailureException;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.LookupIdx;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Differential generative test for {@code INTO}: every step of a random pipeline is run as a plain query, then again with
 * {@code | INTO target} writing its rows to a fresh, dynamically mapped index of a random mode, and the target is read
 * back with {@code FROM target}. The rows read back must be the rows the plain query returned, and the statistics row
 * {@code INTO} answers with must account for every one of them.
 * <p>
 * What the destination keeps is not exactly what the query produced, by design of dynamic mapping, so the comparison
 * normalizes both sides the way {@link CrossIndexModeGenerativeRestRunner} does and adds the round-trip rules: doubles
 * are mapped as {@code float}, strings of every flavour (dates, ips, versions included) come back from a sorted,
 * de-duplicated keyword, and a column that only ever held nulls is never mapped. Columns the destination cannot take
 * (metadata other than {@code _id}, unsupported types) are dropped from both sides, and types that cannot round-trip at
 * all (spatial, unsigned long, exotic aggregates, a double column long enough to be mapped as a quantized
 * {@code dense_vector}) are compared only by presence.
 * <p>
 * Nondeterministic pipelines (sampling, unsorted limits, {@code NOW()}, ...) are not compared, using the same gate as
 * the cross-index-mode test. A write the destination refuses for reasons dynamic mapping makes inevitable (two rows
 * mapping a field to different shapes, an id collision, a time series index without a timestamp) is skipped and
 * counted; any other failure of the write or the read-back fails the test.
 */
public abstract class IntoGenerativeRestRunner extends GenerativeRestTest {

    private static final Logger logger = LogManager.getLogger(IntoGenerativeRestRunner.class);

    /** Pipelines per run; fewer than the plain generative test since every step runs three queries and builds an index. */
    static final int INTO_ITERATIONS = Integer.parseInt(System.getProperty("tests.into.generative.iterations", "40"));

    /** Index modes a target may use; time series needs dimensions and a routing path, so it is out. */
    private static final List<String> MODES = List.of("standard", "lookup", "columnar", "vectordb_document", "vectordb_columnar");
    /** Modes that also need every row to carry a {@code @timestamp}; only used when the query produces one. */
    private static final List<String> TIMESTAMP_MODES = List.of("logsdb", "logsdb_columnar");

    /** Types whose values do not survive dynamic mapping in a comparable form; such columns are compared by presence only. */
    private static final Set<String> PRESENCE_ONLY_TYPES = Set.of(
        "geo_point",
        "cartesian_point",
        "geo_shape",
        "cartesian_shape",
        "flattened",
        "unsigned_long",
        "aggregate_metric_double",
        "dense_vector",
        "tdigest",
        "histogram",
        "exponential_histogram",
        "date_range",
        "double_range"
    );

    /** Types that come back from a keyword, so multi-values are sorted and de-duplicated on both sides. */
    private static final Set<String> STRING_LIKE_TYPES = Set.of("keyword", "text", "ip", "version", "wildcard", "date", "date_nanos");

    /**
     * Refusals a dynamically mapped destination can legitimately produce for a random pipeline. Anything else that
     * stops the write is a failure of the test.
     */
    private static final List<Pattern> ALLOWED_INTO_FAILURES = List.of(
        Pattern.compile("failed to parse field"),
        Pattern.compile("mapper_parsing_exception|document_parsing_exception|strict_dynamic_mapping_exception"),
        Pattern.compile("can't merge|mapping update|Could not dynamically add mapping|object mapping|Limit of total fields"),
        Pattern.compile("already exists; use \"collisions\""),
        Pattern.compile("timestamp field \\[@timestamp\\] is missing"),
        Pattern.compile("SORT has no effect on what INTO writes"),
        Pattern.compile("Field \\[[^]]*\\] .* cannot be used in columnar|doc values|reconstruct"),
        Pattern.compile("has an unsupported type and cannot be written"),
        Pattern.compile("is used as the document id, so it must be")
    );

    private int targets = 0;
    private int compared = 0;
    private int skippedNondeterministic = 0;
    private int skippedRefused = 0;
    private boolean deterministic = true;

    @Before
    public void requiresInto() throws IOException {
        assumeTrue("INTO is behind the esql_into feature flag", RestEsqlTestCase.hasCapabilities(client(), List.of("into_command")));
    }

    @Override
    public void test() throws IOException {
        List<String> indices = availableIndices();
        List<LookupIdx> lookupIndices = lookupIndices();
        Collection<CsvTestsDataLoader.EnrichConfig> policies = CsvTestsDataLoader.ENRICH_POLICIES.values();
        Set<String> viewNames = CsvTestsDataLoader.VIEW_CONFIGS.keySet();
        CommandGenerator.QuerySchema mappingInfo = new CommandGenerator.QuerySchema(indices, lookupIndices, policies, viewNames);

        for (int i = 0; i < INTO_ITERATIONS; i++) {
            var exec = executor();
            try {
                EsqlQueryGenerator.generatePipeline(
                    MAX_DEPTH,
                    sourceCommand(),
                    mappingInfo,
                    exec,
                    requiresTimeSeries(),
                    this,
                    rootGenerationContext()
                );
            } catch (Exception e) {
                if (e instanceof AllowedGeneratorFailureException == false && isAllowedError(e.getMessage()) == false) {
                    String previousQuery = exec.previousResult == null ? null : exec.previousResult.query();
                    throw new AssertionError(
                        "Generative tests, error generating new command\n" + failureReport(previousQuery, e.getMessage()),
                        e
                    );
                }
            }
        }
        logger.info(
            "INTO generative run: compared [{}] steps, skipped [{}] nondeterministic and [{}] refused by the destination",
            compared,
            skippedNondeterministic,
            skippedRefused
        );
        assertTrue("the run compared nothing, so it tested nothing", compared > 0);
    }

    /** Approximation is refused by INTO, and an estimate could not be compared with an exact read-back anyway. */
    @Override
    protected CommandGenerator sourceCommand() {
        return new FromGenerator() {
            @Override
            protected String randomQueryApproximationSettings() {
                return "";
            }
        };
    }

    @Override
    protected QueryExecuted runCommand(QueryExecuted prev, CommandGenerator.CommandDescription current) {
        if (prev == null) {
            deterministic = true;
        }
        QueryExecuted ref = prev == null
            ? execute(current.commandString(), 0)
            : execute(prev.query() + current.commandString(), prev.depth());
        deterministic = CrossIndexModeGenerativeRestRunner.updateDeterminismGate(deterministic, current, ref, prev);
        if (ref.exception() != null || ref.result() == null || ref.outputSchema() == null) {
            return ref;
        }
        if (deterministic == false) {
            skippedNondeterministic++;
            return ref;
        }
        try {
            roundTrip(current, ref);
        } catch (IOException e) {
            throw new AssertionError(failureReport(ref.query(), e.getMessage()), e);
        }
        return ref;
    }

    /** Writes the rows of {@code ref} to a fresh target and checks the statistics row and what is read back. */
    private void roundTrip(CommandGenerator.CommandDescription current, QueryExecuted ref) throws IOException {
        List<Column> written = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        for (Column column : ref.outputSchema()) {
            if (writable(column)) {
                written.add(column);
            } else {
                dropped.add(column.name());
            }
        }
        if (written.isEmpty()) {
            return;
        }
        String mode = randomMode(written);
        String target = "into_target_" + (targets++);
        try {
            createIndex(target, targetSettings(mode), """
                {"dynamic": true, "date_detection": false, "numeric_detection": false}
                """);
        } catch (Exception e) {
            // A mode this cluster does not know; the step is not lost, just written to a plain index.
            logger.info("could not create a [{}] target ({}), using standard", mode, e.getMessage());
            mode = "standard";
            createIndex(target, targetSettings(mode), """
                {"dynamic": true, "date_detection": false, "numeric_detection": false}
                """);
        }
        try {
            StringBuilder producer = new StringBuilder(ref.query());
            if (dropped.isEmpty() == false) {
                producer.append(" | DROP ");
                for (int i = 0; i < dropped.size(); i++) {
                    producer.append(i == 0 ? "" : ", ").append('`').append(dropped.get(i).replace("`", "``")).append('`');
                }
            }
            // The plain query is capped by the implicit limit; the write is not, so the cap is made explicit. The
            // determinism gate has already ruled out the case where it bit.
            producer.append(" | LIMIT 1000");
            QueryExecuted into = execute(producer + " | INTO " + target, ref.depth());
            if (into.exception() != null) {
                String message = into.exception().getMessage();
                if (isAllowedError(message) || ALLOWED_INTO_FAILURES.stream().anyMatch(p -> p.matcher(message).find())) {
                    skippedRefused++;
                    logger.info("INTO [{}] refused {}: {}", mode, ref.query(), shortReason(into.exception()));
                    return;
                }
                throw new AssertionError(
                    "INTO failed where the plain query succeeded\n  mode: " + mode + "\n" + failureReport(into.query(), message)
                );
            }
            compared++;
            checkStats(into, ref.result().size(), mode);

            boolean hasId = written.stream().anyMatch(c -> c.name().equals("_id"));
            QueryExecuted back = execute("FROM " + target + (hasId ? " METADATA _id" : "") + " | LIMIT 1000", ref.depth());
            if (back.exception() != null
                && back.exception().getMessage().contains("missing references [<no-fields>")
                && allNull(ref.result())) {
                // Nothing was mapped, since every value was null, and FROM on an index that holds documents but no
                // fields trips an ES|QL planner check in every index mode. That has nothing to do with INTO.
                logger.info("skipping the read-back of field-less [{}] target for {}", mode, ref.query());
                return;
            }
            if (back.exception() != null) {
                throw new AssertionError(
                    "reading the INTO target back failed\n  mode: "
                        + mode
                        + "\n"
                        + failureReport(back.query(), back.exception().getMessage())
                );
            }
            compareRows(current, ref, written, back, mode);
        } finally {
            deleteIndex(target);
        }
    }

    /** The reason out of a REST error body, which is otherwise a screenful of JSON (or binary) and stack trace. */
    private static String shortReason(Exception e) {
        if (e instanceof ResponseException response && response.getResponse().getEntity() != null) {
            try {
                HttpEntity entity = response.getResponse().getEntity();
                XContentType type = XContentType.fromMediaType(entity.getContentType().getValue());
                Map<String, Object> body = EsqlTestUtils.entityToMap(entity, type);
                Object error = body.get("error");
                if (error instanceof Map<?, ?> map && map.get("reason") != null) {
                    return map.get("reason").toString();
                }
            } catch (Exception ignored) {
                // Fall through to the message.
            }
        }
        return e.getMessage().lines().findFirst().orElse(e.getMessage());
    }

    private static boolean allNull(List<List<Object>> rows) {
        for (List<Object> row : rows) {
            for (Object cell : row) {
                if (cell != null) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Settings targetSettings(String mode) {
        Settings.Builder settings = Settings.builder().put("index.number_of_replicas", 0);
        if (mode.equals("standard") == false) {
            settings.put("index.mode", mode);
        }
        if (mode.equals("lookup")) {
            settings.put("index.number_of_shards", 1);
        }
        return settings.build();
    }

    private static String randomMode(List<Column> written) {
        boolean hasTimestamp = written.stream().anyMatch(c -> c.name().equals("@timestamp") && c.type().equals("date"));
        List<String> modes = new ArrayList<>(MODES);
        if (hasTimestamp) {
            modes.addAll(TIMESTAMP_MODES);
        }
        return randomFrom(modes);
    }

    /** Metadata columns other than {@code _id} and unsupported types cannot be written, so they are dropped on both sides. */
    private static boolean writable(Column column) {
        if (column.name().equals("_id")) {
            return true;
        }
        return column.name().startsWith("_") == false && column.type().equals("unsupported") == false;
    }

    /** The statistics row must account for every row of the plain query, all of them created, none refused. */
    private static void checkStats(QueryExecuted into, int rows, String mode) {
        List<String> names = into.outputSchema().stream().map(Column::name).toList();
        assertEquals(
            "INTO answers with its statistics row\n  query: " + into.query(),
            List.of("total", "created", "updated", "version_conflicts", "batches", "failures", "failure_messages"),
            names
        );
        assertEquals("one statistics row\n  query: " + into.query(), 1, into.result().size());
        List<Object> stats = into.result().get(0);
        String context = "\n  mode: " + mode + "\n  stats: " + stats + "\n  query: " + into.query();
        assertEquals("total" + context, rows, ((Number) stats.get(0)).intValue());
        assertEquals("created" + context, rows, ((Number) stats.get(1)).intValue());
        assertEquals("updated" + context, 0, ((Number) stats.get(2)).intValue());
        assertEquals("version_conflicts" + context, 0, ((Number) stats.get(3)).intValue());
        assertEquals("batches" + context, rows == 0, ((Number) stats.get(4)).intValue() == 0);
        assertEquals("failures" + context, 0, ((Number) stats.get(5)).intValue());
        assertNull("failure_messages" + context, stats.get(6));
    }

    /**
     * The rows read back must be the rows written, as a multiset, after both sides are normalized for what dynamic
     * mapping does to values: see the class comment.
     */
    private void compareRows(
        CommandGenerator.CommandDescription current,
        QueryExecuted ref,
        List<Column> written,
        QueryExecuted back,
        String mode
    ) {
        List<Column> backSchema = back.outputSchema();
        String context = "\n  mode: " + mode + "\n  reference query: " + ref.query() + "\n  read-back query: " + back.query();

        // Every column read back is one that was written, or a keyword multi-field dynamic mapping added to a string.
        Set<String> writtenNames = new java.util.HashSet<>();
        written.forEach(c -> writtenNames.add(c.name()));
        for (Column column : backSchema) {
            String name = column.name();
            if (writtenNames.contains(name) == false
                && (name.endsWith(".keyword") && writtenNames.contains(name.substring(0, name.length() - 8))) == false) {
                fail("the target holds a column [" + name + "] that was never written" + context);
            }
        }

        // Rows restricted to the written columns, in that order; a column the target never mapped only ever held nulls.
        int[] refIndex = new int[written.size()];
        int[] backIndex = new int[written.size()];
        List<Column> compareSchema = new ArrayList<>(written.size());
        for (int i = 0; i < written.size(); i++) {
            Column column = written.get(i);
            refIndex[i] = indexOf(ref.outputSchema(), column.name());
            backIndex[i] = indexOf(backSchema, column.name());
            // A double column whose rows hold enough values (32 in the vectordb modes, 128 elsewhere) is dynamically
            // mapped as a dense_vector, which quantizes what it stores; such a column is compared by presence only.
            String type = backIndex[i] >= 0 && backSchema.get(backIndex[i]).type().equals("dense_vector") ? "dense_vector" : column.type();
            compareSchema.add(new Column(column.name(), compareType(type), column.originalTypes()));
            if (backIndex[i] < 0) {
                for (List<Object> row : ref.result()) {
                    if (row.get(refIndex[i]) != null) {
                        fail("column [" + column.name() + "] was written but is missing from the target" + context);
                    }
                }
            }
        }
        List<List<Object>> refRows = project(ref.result(), refIndex, compareSchema, true);
        List<List<Object>> backRows = project(back.result(), backIndex, compareSchema, false);
        assertEquals("row count read back" + context, refRows.size(), backRows.size());

        List<List<String>> refCells = CrossIndexModeGenerativeRestRunner.toCanonicalCells(refRows, compareSchema);
        List<List<String>> backCells = CrossIndexModeGenerativeRestRunner.toCanonicalCells(backRows, compareSchema);
        List<String> refCanon = new ArrayList<>(refCells.size());
        List<String> backCanon = new ArrayList<>(backCells.size());
        refCells.forEach(cells -> refCanon.add(String.join(" | ", cells)));
        backCells.forEach(cells -> backCanon.add(String.join(" | ", cells)));
        Collections.sort(refCanon);
        Collections.sort(backCanon);
        if (refCanon.equals(backCanon)
            || CrossIndexModeGenerativeRestRunner.rowsMatchWithinRoundingTolerance(refCells, backCells, compareSchema)) {
            return;
        }
        for (int row = 0; row < refCanon.size(); row++) {
            if (refCanon.get(row).equals(backCanon.get(row)) == false) {
                fail(
                    "INTO round trip divergence at sorted row "
                        + row
                        + " in ["
                        + current.commandName()
                        + "]:\n  written:   "
                        + refCanon.get(row)
                        + "\n  read back: "
                        + backCanon.get(row)
                        + "\n  columns: "
                        + compareSchema.stream().map(c -> c.name() + ":" + c.type()).toList()
                        + context
                );
            }
        }
    }

    /** Picks the compared columns out of each row; on the reference side, doubles are narrowed to the float they were mapped as. */
    private static List<List<Object>> project(List<List<Object>> rows, int[] indices, List<Column> schema, boolean reference) {
        List<List<Object>> projected = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            List<Object> cells = new ArrayList<>(indices.length);
            for (int i = 0; i < indices.length; i++) {
                Object value = indices[i] < 0 ? null : row.get(indices[i]);
                if (value instanceof List<?> values) {
                    // Strings come back from a keyword, which keeps one copy of each value; and a one-element list is
                    // the same document as a scalar once indexed.
                    List<Object> kept = schema.get(i).type().equals("keyword")
                        ? new ArrayList<>(new java.util.LinkedHashSet<>(values))
                        : new ArrayList<>(values);
                    value = kept.size() == 1 ? kept.get(0) : kept;
                }
                if (reference && schema.get(i).type().equals("double")) {
                    value = narrowToFloat(value);
                }
                cells.add(value);
            }
            projected.add(cells);
        }
        return projected;
    }

    private static Object narrowToFloat(Object value) {
        if (value instanceof Number number) {
            return (double) (float) number.doubleValue();
        }
        if (value instanceof List<?> values) {
            List<Object> narrowed = new ArrayList<>(values.size());
            values.forEach(v -> narrowed.add(narrowToFloat(v)));
            return narrowed;
        }
        return value;
    }

    /** The type the comparison treats a written column as: what dynamic mapping turned it into, as far as values go. */
    private static String compareType(String type) {
        String lower = type.toLowerCase(Locale.ROOT);
        if (PRESENCE_ONLY_TYPES.contains(lower)) {
            // Compared by presence only, like the cross-mode test does for spatial columns.
            return "geo_point";
        }
        if (STRING_LIKE_TYPES.contains(lower)) {
            return "keyword";
        }
        return lower;
    }

    private static int indexOf(List<Column> schema, String name) {
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
