/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.core.Releasable;

import java.util.function.Supplier;

public class Driver implements Supplier<Page> {

    private final Operator operator;
    private final Releasable releasable;

    public Driver(Operator operator, Releasable releasable) {
        this.operator = operator;
        this.releasable = releasable;
    }

    @Override
    public Page get() {
        do {
            Page page = operator.getOutput();
            if (page != null) {
                return page;
            }
        } while (operator.isFinished() == false);

        releasable.close();
        return null;
    }
}
