/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.qlcore.expression.predicate;

import org.elasticsearch.xpack.qlcore.expression.Expression;

public interface Negatable<T extends Expression> {

    T negate();

}
