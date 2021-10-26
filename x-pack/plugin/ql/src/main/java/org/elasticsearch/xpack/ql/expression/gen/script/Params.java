/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.gen.script;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Parameters for a script
 *
 * This class mainly exists to handle the different aggregation cases.
 * While aggs can appear in scripts like regular parameters, they are not passed
 * as parameters but rather as bucket_path.
 * However in some cases (like count), it's not the agg path that is relevant but rather
 * its property (_count).
 * As the agg name still needs to be remembered to properly associate the script with.
 *
 * Hence why this class supports aggRef (which always returns the agg names) and aggPaths
 * (which returns the agg property if it exists or the agg name/reference).
 *
 * Also the parameter names support late binding/evaluation since the agg reference (like function id)
 * can be changed during the optimization phase (for example min agg -&gt; stats.min).
 */
public class Params {

    public static final Params EMPTY = new Params(emptyList());

    private final List<Param<?>> params;

    Params(List<Param<?>> params) {
        // flatten params
        this.params = flatten(params);
    }

    // return vars and aggs in the declared order for binding them to the script
    List<String> asCodeNames() {
        if (params.isEmpty()) {
            return emptyList();
        }

        List<String> names = new ArrayList<>(params.size());
        int aggs = 0, vars = 0;

        for (Param<?> p : params) {
            names.add(p.prefix() + (p instanceof Agg ? aggs++ : vars++));
        }

        return names;
    }

    // return only the vars (as parameter for a script)
    // agg refs are returned separately to be provided as bucket_paths
    Map<String, Object> asParams() {
        Map<String, Object> map = new LinkedHashMap<>(params.size());

        int count = 0;

        for (Param<?> p : params) {
            if (p instanceof Var) {
                map.put(p.prefix() + count++, p.value());
            }
        }

        return map;
    }

    // return agg refs in a format suitable for bucket_paths
    Map<String, String> asAggPaths() {
        Map<String, String> map = new LinkedHashMap<>();

        int aggs = 0;

        for (Param<?> p : params) {
            if (p instanceof Agg) {
                Agg a = (Agg) p;
                String s = a.aggProperty() != null ? a.aggProperty() : a.aggName();
                map.put(p.prefix() + aggs++, s);
            }
            if (p instanceof Grouping) {
                Grouping g = (Grouping) p;
                map.put(p.prefix() + aggs++, g.groupName());
            }
        }

        return map;
    }

    private static List<Param<?>> flatten(List<Param<?>> params) {
        List<Param<?>> flatten = emptyList();

        if (params.isEmpty() == false) {
            flatten = new ArrayList<>();
            for (Param<?> p : params) {
                if (p instanceof Script) {
                    flatten.addAll(flatten(((Script) p).value().params));
                }
                else if (p instanceof Agg) {
                    flatten.add(p);
                }
                else if (p instanceof Grouping) {
                    flatten.add(p);
                }
                else if (p instanceof Var) {
                    flatten.add(p);
                }
                else {
                    throw new QlIllegalArgumentException("Unsupported field {}", p);
                }
            }
        }
        return flatten;
    }

    @Override
    public String toString() {
        return params.toString();
    }

    @Override
    public int hashCode() {
        return this.params.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof  Params) == false) {
            return false;
        }
        return this.params.equals(((Params)obj).params);
    }
}
