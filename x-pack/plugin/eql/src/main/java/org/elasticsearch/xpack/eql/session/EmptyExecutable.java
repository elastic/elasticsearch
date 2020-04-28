/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.Objects;

public class EmptyExecutable implements Executable {

    private final List<Attribute> output;

    public EmptyExecutable(List<Attribute> output) {
        this.output = output;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void execute(EqlSession session, ActionListener<Results> listener) {
        listener.onResponse(Results.EMPTY);
    }

    @Override
    public int hashCode() {
        return output.hashCode();
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
