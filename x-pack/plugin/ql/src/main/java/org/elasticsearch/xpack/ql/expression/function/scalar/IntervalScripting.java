/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar;

// FIXME: accessor interface until making script generation pluggable
public interface IntervalScripting {

    String script();

    String value();

    String typeName();

}
