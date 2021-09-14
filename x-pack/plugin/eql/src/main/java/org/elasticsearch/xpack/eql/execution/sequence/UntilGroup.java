/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.function.Function;

public class UntilGroup extends OrdinalGroup<Ordinal> {

    UntilGroup() {
        super(Function.identity());
    }
}
