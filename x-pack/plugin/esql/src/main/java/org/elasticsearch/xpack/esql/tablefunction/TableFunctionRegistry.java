/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The table functions a source list may call, by name.
 * <p>
 * Resolution happens while the plan is built, so an unrecognized name fails as a parse error naming the
 * functions that do exist, rather than reaching index resolution and failing as a missing index. That
 * also means the set is fixed at startup and identical on every node: nothing here consults cluster
 * state, and a call's name carries no privilege of its own.
 */
public final class TableFunctionRegistry {

    /**
     * The registry every query is planned against. Add a function by adding it to this list — the name
     * is read from the function itself, so there is no second place for the two to disagree.
     */
    public static final TableFunctionRegistry INSTANCE = new TableFunctionRegistry(List.of(new DatasetTableFunction()));

    private final Map<String, TableFunction> byName;

    TableFunctionRegistry(List<TableFunction> functions) {
        // TreeMap with a case-insensitive comparator: a user may write DATASET or dataset, as they may for
        // every other name in the language, and the sorted key order is what unknownFunction lists.
        Map<String, TableFunction> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (TableFunction function : functions) {
            String name = function.name();
            if (name.equals(name.toLowerCase(Locale.ROOT)) == false) {
                throw new IllegalStateException("table function name [" + name + "] must be lowercase");
            }
            TableFunction previous = map.put(name, function);
            if (previous != null) {
                throw new IllegalStateException("duplicate table function [" + name + "]");
            }
        }
        this.byName = map;
    }

    /**
     * The function the given name resolves to, case-insensitively.
     *
     * @throws ParsingException if no function has that name
     */
    public TableFunction resolve(Source source, String name) {
        TableFunction function = byName.get(name);
        if (function == null) {
            throw new ParsingException(
                source,
                "unknown table function [{}], expected one of [{}]",
                name,
                String.join(", ", byName.keySet())
            );
        }
        return function;
    }
}
