/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

public class QuerySettings {

    private final List<Alias> fields;

    public QuerySettings(Source source, List<Alias> fields) {
        this.fields = fields;
    }

    public List<Alias> fields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QuerySettings eval = (QuerySettings) o;
        return Objects.equals(fields, eval.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

}
