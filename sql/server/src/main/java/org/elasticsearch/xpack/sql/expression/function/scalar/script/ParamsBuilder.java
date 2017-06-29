/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.script;

import java.util.ArrayList;
import java.util.List;

public class ParamsBuilder {

    private final List<Param<?>> params = new ArrayList<>();

    public static ParamsBuilder paramsBuilder() {
        return new ParamsBuilder();
    }

    public ParamsBuilder variable(Object value) {
        params.add(new Var(value));
        return this;
    }

    public ParamsBuilder agg(String aggName) {
        return agg(aggName, null);
    }

    public ParamsBuilder agg(String aggName, String aggPath) {
        params.add(new Agg(aggName, aggPath));
        return this;
    }

    public ParamsBuilder script(Params ps) {
        params.add(new Script(ps));
        return this;
    }

    public Params build() {
        return new Params(new ArrayList<>(params));
    }
}