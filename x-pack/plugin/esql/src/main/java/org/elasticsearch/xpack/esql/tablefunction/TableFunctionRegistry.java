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
 * Built once at startup and identical on every node: nothing here consults cluster state, and a call's name
 * carries no privilege of its own. Built from a list of already-constructed functions, so a function that needs
 * collaborators is handed them where it is created and nothing shared has to know about them.
 * <p>
 * <b>It may legitimately be empty.</b> A node registering no function is a working state, not a half-built one,
 * and {@code resolve} answers for that case in its own words rather than listing an empty set.
 */
public final class TableFunctionRegistry {

    private final Map<String, TableFunction> byName;

    public TableFunctionRegistry(List<TableFunction> functions) {
        // TreeMap with a case-insensitive comparator: a user may write a name in any case, as they may for every
        // other name in the language, and the sorted key order is what an unknown-name error lists.
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
            // Its own arm when nothing is registered: "expected one of []" reads as a bug in the engine rather
            // than as the answer to what the user asked, and on a build with no function registered it is the
            // only message this method can produce.
            if (byName.isEmpty()) {
                throw new ParsingException(source, "no table functions are registered, so [{}] cannot be used here", name);
            }
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
