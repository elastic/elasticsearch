/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.script;

class Agg extends Param<String> {

    private final String aggProperty;

    Agg(String aggRef, String aggProperty) {
        super(aggRef);
        this.aggProperty = aggProperty;
    }

    public String aggProperty() {
        return aggProperty;
    }

    @Override
    public String prefix() {
        return "a";
    }
}