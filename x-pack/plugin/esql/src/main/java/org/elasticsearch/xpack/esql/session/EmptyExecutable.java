/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class EmptyExecutable implements LocalExecutable {

    private final List<Attribute> output;

    public EmptyExecutable(List<Attribute> output) {
        this.output = output;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void execute(EsqlSession session, ActionListener<Result> listener) {
        listener.onResponse(new Result(output, emptyList()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EmptyExecutable other = (EmptyExecutable) obj;
        return Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return output.toString();
    }
}
