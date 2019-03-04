/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.script;

class Script extends Param<Params> {

    Script(Params value) {
        super(value);
    }

    @Override
    public String prefix() {
        return "s";
    }
}