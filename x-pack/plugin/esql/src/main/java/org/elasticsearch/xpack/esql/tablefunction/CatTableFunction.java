/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code FROM CAT("shards", "logs-*")} — a {@code _cat} table as a relation.
 * <p>
 * {@code _cat} answers questions about the cluster as a text table nothing can read, so "how many shards of this
 * pattern does each node hold" is a shell pipeline today and a query here:
 *
 * <pre>{@code
 * FROM CAT("shards", "logs-*") | STATS copies = COUNT(*) BY node | SORT copies DESC
 * }</pre>
 *
 * The columns are {@code RestShardsAction.getTableWithHeader} and the rows are its {@code buildTable}, called with
 * the two responses {@code _cat/shards} dispatches, so the column list and the default rendering of a cell have
 * one definition rather than two. The row sets can still differ: this resolves system indices at ES|QL's access
 * level rather than the endpoint's.
 * <p>
 * Every column is {@code keyword}, because a {@code _cat} cell is text and the header carries no type. The plain
 * counters — {@code shard}, {@code docs}, the {@code *.total} and {@code *.count} columns — cast to a number;
 * the sizes and durations do not, because {@code store} is the text {@code 227b} and no ES|QL expression parses a
 * byte size or a human duration. {@code SORT shard} is lexical for the same reason.
 */
public class CatTableFunction extends CoordinatorRowsFunction {

    public static final String NAME = "cat";

    /** The one table so far. */
    private static final String SHARDS = "shards";

    private static final TableFunctionSignature SIGNATURE = TableFunctionSignature.of(
        TableFunctionSignature.name("table"),
        TableFunctionSignature.name("index pattern")
    );

    /** A row per shard copy, not per document. Past it the read is refused rather than materialized. */
    private static final int MAX_ROWS = 10_000;

    /** Bounds waiting for the master, not the work; {@code _cat/shards} passes the same one. */
    private static final TimeValue MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final Client client;

    public CatTableFunction(Client client) {
        this.client = client;
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
        if (SHARDS.equalsIgnoreCase(call.name(0)) == false) {
            throw new ParsingException(
                call.arguments().get(0).source(),
                "[{}] has no table named [{}], expected one of [{}]",
                NAME,
                call.name(0),
                SHARDS
            );
        }
        // Both halves: " " is blank but splits to one element, and "," is not blank but splits to none - and an
        // empty indices array on these requests means every index in the cluster.
        if (call.name(1).isBlank() || Strings.splitStringByCommaToArray(call.name(1)).length == 0) {
            throw new ParsingException(call.arguments().get(1).source(), "[{}] requires an index pattern, found an empty one", NAME);
        }
    }

    /**
     * The two requests {@code _cat/shards} makes, which is also its authorization. The rows come from the
     * cluster-state half, a {@code cluster:monitor/state} action that is not narrowed per index — so the row set
     * is every shard matching the pattern, for a caller holding {@code monitor} and no index privilege at all.
     * The stats half is narrowed, so its columns are null for an index the caller cannot read, and a concrete
     * name they cannot see is refused rather than omitted. No test covers this; see the PR body.
     */
    @Override
    protected void write(TableFunctionInvocation call, ProjectMetadata projectMetadata, TableRows rows, ActionListener<Void> listener) {
        String[] indices = Strings.splitStringByCommaToArray(call.name(1));
        ClusterStateRequest stateRequest = new ClusterStateRequest(MASTER_NODE_TIMEOUT);
        stateRequest.clear().nodes(true).routingTable(true).indices(indices).indicesOptions(IndicesOptions.strictExpandHidden());
        IndicesStatsRequest statsRequest = new IndicesStatsRequest().all()
            .indices(indices)
            .indicesOptions(IndicesOptions.strictExpandHidden());

        client.admin()
            .cluster()
            .state(
                stateRequest,
                listener.delegateFailureAndWrap((stateDone, state) -> client.admin().indices().stats(statsRequest, stateDone.map(stats -> {
                    write(call.name(1), RestShardsAction.buildTable(RestApiVersion.current(), state, stats), rows);
                    return null;
                })))
            );
    }

    /**
     * Every {@code _cat} column is text, because that is what {@code _cat} renders — a shard's store arrives as
     * {@code "227b"} rather than as a number. Written over a {@link Table} rather than over the two responses, so
     * it can be driven without a cluster.
     */
    static void write(String pattern, Table table, TableRows rows) {
        if (table.getRows().size() > MAX_ROWS) {
            // An IllegalArgumentException so this reaches the user as a 400: too many shards is their input and
            // their fix.
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] matched ["
                    + table.getRows().size()
                    + "] shards for ["
                    + pattern
                    + "], more than the ["
                    + MAX_ROWS
                    + "] it will return - narrow the index pattern"
            );
        }
        List<TableRows.Keyword> columns = new ArrayList<>(table.getHeaders().size());
        for (Table.Cell header : table.getHeaders()) {
            columns.add(rows.keyword(String.valueOf(header.value)));
        }
        for (List<Table.Cell> cells : table.getRows()) {
            for (int c = 0; c < columns.size(); c++) {
                Object value = cells.get(c).value;
                // A null cell is an absent value, not the string "null".
                columns.get(c).appendOrNull(value == null ? null : String.valueOf(value));
            }
        }
    }
}
