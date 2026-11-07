/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executor;

/**
 * {@code FROM FIELDS("logs-*")} — the fields of an index pattern, as rows, with how each one disagrees across the indices that have it.
 *
 * <h2>What this makes answerable</h2>
 *
 * The disagreement. {@code FROM logs-* | LIMIT 0} already names every field and its type, so enumeration is not what earns this its
 * place — but a field mapped {@code keyword} in one index and {@code text} in another reaches that answer as the single word
 * {@code unsupported}, because {@code InvalidMappedField} hands {@code DataType.UNSUPPORTED} to its superclass constructor and from
 * there nothing distinguishes it from a field whose type ES|QL never supported. Which types disagree, and which indices hold them, is
 * carried by {@link TypeConflictedField#errorMessage()} and {@link TypeConflictedField#getTypesToIndices()}, and both are reachable
 * today only by writing a query that fails on the field and reading the error. Neither is data, so neither can be filtered, grouped or
 * counted. That is what the {@code conflict} and {@code conflicting_types} columns are for.
 *
 * <h2>The argument</h2>
 *
 * One index pattern, and the function can do nothing without it: no index holds the mapping of {@code logs-*}, so there is no relation
 * to enumerate and then narrow with a {@code WHERE}. A pattern needs quoting whenever it is not a bare identifier, because
 * {@code tableFunctionArgument} admits {@code identifier | string} and {@code UNQUOTED_ID_BODY} is letters, digits and underscore — so
 * {@code FIELDS(logs)} parses and {@code FIELDS(logs-*)} does not.
 *
 * <h2>Authorization, and why there is no new notion of it here</h2>
 *
 * This resolves the pattern through {@link IndexResolver}, which dispatches {@code EsqlResolveFieldsAction}
 * ({@code indices:data/read/esql/resolve_fields}) — the same action every {@code FROM} in the language dispatches, in the caller's own
 * thread context. The request is an {@code IndicesRequest.Replaceable} resolved under lenient indices options, so the security layer
 * narrows it exactly as it narrows a query: a caller sees the fields of the indices they may read, and the indices they may not are
 * absent rather than refused. The function names no privilege and reads no role. A caller with partial access therefore sees a
 * conflict only among the indices they can read, which is the same mapping a query of theirs would be planned against.
 */
public class FieldsTableFunction extends CoordinatorRowsFunction {

    public static final String NAME = "fields";

    private static final TableFunctionSignature SIGNATURE = TableFunctionSignature.of(TableFunctionSignature.name("index pattern"));

    /**
     * The columns, and why these.
     * <p>
     * {@code name} and {@code type} are what makes the relation usable at all — a conflict row that cannot say which field it is about
     * is useless — and they are the two that {@code LIMIT 0} already gives. {@code conflicting_types} is the complete set of source
     * types a field was mapped as, which is what {@code STATS ... BY conflicting_types} groups on. {@code conflict} is the rendered
     * detail, including the disagreeing index names.
     * <p>
     * There is no column of the disagreeing indices, because one could not be produced honestly: on any cluster recent enough for
     * {@code CompactInvalidMappedField}, the resolver keeps at most three index names per source type and a count of the rest, so a
     * column of them would be silently truncated and would carry {@code "..."} as if it were an index name. {@code conflict} carries
     * the same names in the one form that states its own cap ("and [N] other indices").
     * <p>
     * Nor is there a column for {@code EsField#isAlias}, {@code EsField#isAggregatable} or the time-series field type.
     * {@code isAlias} is set by the resolver on the object parents it synthesizes for dotted names, not on field aliases, so the
     * column would not mean what its name says. {@code isAggregatable} is false by construction on every conflicted field, which is
     * every row that matters here. The time-series field type is {@code NONE} for every field unless the resolution was asked for on
     * behalf of a time-series aggregation, which this one is not, so the column would be a constant.
     */
    private static final List<Attribute> COLUMNS = List.of(
        column("name", DataType.KEYWORD),
        column("type", DataType.KEYWORD),
        column("conflict", DataType.KEYWORD),
        column("conflicting_types", DataType.KEYWORD)
    );

    private final IndexResolver indexResolver;
    private final Executor planningExecutor;

    /**
     * @param client dispatches the field resolution as the caller, and names the pool the answer is handed back on, which together are
     *               the whole of what this function needs from the node
     */
    public FieldsTableFunction(Client client) {
        // The flattened kill switch (esql.query.flattened.enabled) is not reachable from what a table function is given, and it governs
        // whether a query may read a flattened field rather than whether one is mapped. This function reads no value, so it reports the
        // mapped type; under a thrown switch that is what the mapping says and not what a query would see.
        this(new IndexResolver(client, () -> true), client.threadPool().executor(ThreadPool.Names.SEARCH));
    }

    FieldsTableFunction(IndexResolver indexResolver, Executor planningExecutor) {
        this.indexResolver = indexResolver;
        this.planningExecutor = planningExecutor;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TableFunctionSignature signature() {
        return SIGNATURE;
    }

    @Override
    public void validate(TableFunctionInvocation call) {
        if (call.name(0).isBlank()) {
            throw new ParsingException(call.arguments().get(0).source(), "[{}] requires an index pattern, found an empty one", NAME);
        }
    }

    @Override
    protected List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context) {
        return COLUMNS;
    }

    /**
     * Forks back onto the planning pool before completing, which is this function's own obligation and not the facility's to discharge.
     * <p>
     * The field-caps merge asserts {@code search_coordination} because it is too expensive for a transport worker, so the resolution
     * arrives on that pool; {@code EsqlSession} resumes planning on {@code search} and asserts as much the moment the facility hands
     * the plan back. Nothing between the two restores the thread, so a function that does I/O has to. The precedent is
     * {@code DatasetResolver}, which wraps its own round-trip in the same way for the same reason.
     */
    @Override
    protected void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener) {
        indexResolver.resolveAllFields(
            call.name(0),
            new ThreadedActionListener<>(planningExecutor, listener.map(FieldsTableFunction::rows))
        );
    }

    /**
     * One row per field of the merged mapping, named by its full path.
     * <p>
     * Walks the whole tree rather than its roots, because the resolver returns a hierarchy: an object's children and a text field's
     * {@code .keyword} both sit in the parent's properties, and they are queryable names a reader is asking about. An object is a row
     * of its own too — it has a name in the mapping, it is the only place a reader learns the shape, and a conflict can sit on a name
     * another index maps as a field — so it is reported with its type rather than silently dropped, leaving {@code WHERE type !=
     * "object"} to whoever does not want it.
     * <p>
     * Sorted by name, because the resolver's root map is a {@code HashMap} and a relation whose row order varies between identical
     * calls is worse to read and worse to test than one that does not.
     */
    static List<List<Object>> rows(IndexResolution resolution) {
        List<List<Object>> rows = new ArrayList<>();
        collect(resolution.get().mapping(), "", rows);
        rows.sort(Comparator.comparing(row -> (String) row.get(0)));
        return rows;
    }

    private static void collect(Map<String, EsField> fields, String prefix, List<List<Object>> rows) {
        for (Map.Entry<String, EsField> entry : fields.entrySet()) {
            String name = prefix + entry.getKey();
            EsField field = entry.getValue();
            // Arrays.asList rather than List.of: a field with nothing wrong with it has no conflict, and List.of refuses a null element.
            rows.add(Arrays.asList(name, field.getDataType().typeName(), conflict(field), conflictingTypes(field)));
            Map<String, EsField> properties = field.getProperties();
            if (properties != null) {
                collect(properties, name + ".", rows);
            }
        }
    }

    private static String conflict(EsField field) {
        return field instanceof TypeConflictedField conflicted ? conflicted.errorMessage() : null;
    }

    private static String conflictingTypes(EsField field) {
        // Sorted and joined rather than multivalued: a single string groups and compares, where a multivalue would answer == with null.
        return field instanceof TypeConflictedField conflicted
            ? String.join(", ", new TreeSet<>(conflicted.getTypesToIndices().keySet()))
            : null;
    }
}
