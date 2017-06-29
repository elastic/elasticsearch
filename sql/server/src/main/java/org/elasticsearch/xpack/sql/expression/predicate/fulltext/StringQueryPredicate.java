/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.fulltext;

import java.util.Map;

import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

public class StringQueryPredicate extends FullTextPredicate {

    private final Map<String, Float> fields;
    private final Operator defaultOperator;

    public StringQueryPredicate(Location location, String query, String options) {
        super(location, query, options, emptyList());

        // inferred
        this.fields = FullTextUtils.parseFields(optionMap(), location);
        this.defaultOperator = FullTextUtils.operator(optionMap(), "default_operator");
    }

    public Map<String, Float> fields() {
        return fields;
    }

    public Operator defaultOperator() {
        return defaultOperator;
    }
}
