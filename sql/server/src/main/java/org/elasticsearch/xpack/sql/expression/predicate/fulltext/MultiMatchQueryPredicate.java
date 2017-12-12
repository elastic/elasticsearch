/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.fulltext;

import java.util.Map;
import java.util.Objects;

import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

public class MultiMatchQueryPredicate extends FullTextPredicate {

    private final String fieldString;
    private final Map<String, Float> fields;
    private final Operator operator;

    public MultiMatchQueryPredicate(Location location, String fieldString, String query, String options) {
        super(location, query, options, emptyList());
        this.fieldString = fieldString;

        // inferred
        this.fields = FullTextUtils.parseFields(fieldString, location);
        this.operator = FullTextUtils.operator(optionMap(), "operator");
    }

    public String fieldString() {
        return fieldString;
    }
    
    public Map<String, Float> fields() {
        return fields;
    }

    public Operator operator() {
        return operator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldString, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MultiMatchQueryPredicate other = (MultiMatchQueryPredicate) obj;
            return Objects.equals(fieldString, other.fieldString);
        }
        return false;
    }
}
