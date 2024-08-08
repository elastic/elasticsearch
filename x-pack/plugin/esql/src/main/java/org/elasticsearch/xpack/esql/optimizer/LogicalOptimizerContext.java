/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Objects;

public class LogicalOptimizerContext {
    private final Configuration configuration;

    public LogicalOptimizerContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration configuration() {
        return configuration;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LogicalOptimizerContext) obj;
        return Objects.equals(this.configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration);
    }

    @Override
    public String toString() {
        return "LogicalOptimizerContext[" + "configuration=" + configuration + ']';
    }

}
