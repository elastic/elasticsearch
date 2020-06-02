/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.session;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ql.expression.Attribute;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class EmptyExecutable implements Executable {

    private final List<Attribute> output;
    private final Results.Type resultType;

    public EmptyExecutable(List<Attribute> output, Results.Type resultType) {
        this.output = output;
        this.resultType = resultType;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public void execute(EqlSession session, ActionListener<Results> listener) {
        listener.onResponse(new Results(new TotalHits(0, Relation.EQUAL_TO), TimeValue.ZERO, false, emptyList(), resultType));
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, resultType);
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
        return Objects.equals(resultType, other.resultType)
                && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return output.toString();
    }
}
