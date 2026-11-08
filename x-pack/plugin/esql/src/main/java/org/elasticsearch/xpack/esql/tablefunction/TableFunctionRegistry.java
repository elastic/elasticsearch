/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
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
 * <b>It may legitimately be empty.</b> A node registering no function is a working state, which is what makes
 * the separation checkable rather than asserted: everything below is exercised by a function defined in the
 * tests, so nothing here can be shaped by one real function's needs.
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
     * Adds one capability per registered function, named {@code table_fn_<name>}, so a client can ask whether this
     * cluster has a particular function and not only whether it has the facility at all. Emits nothing while no
     * function is registered. The facility's own
     * capability stays separate ({@link EsqlCapabilities.Cap#FROM_TABLE_FUNCTION}), because the two become
     * different questions the moment there is a second function.
     * <p>
     * Derived from the registry rather than written out by hand, as
     * {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry#addCapabilities} does it:
     * registering a function is then the whole change, with no second place to forget. Each is enabled with the
     * facility, since a function is unreachable without it.
     */
    public void addCapabilities(EsqlCapabilities.Builder capabilities) {
        boolean enabled = EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled();
        for (String name : byName.keySet()) {
            capabilities.add("table_fn_" + name, enabled);
        }
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
