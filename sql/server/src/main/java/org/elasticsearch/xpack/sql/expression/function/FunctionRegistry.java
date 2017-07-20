/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.session.SqlSettings;

import java.util.Collection;

public interface FunctionRegistry {

    Function resolveFunction(UnresolvedFunction ur, SqlSettings settings);

    boolean functionExists(String name);

    Collection<FunctionDefinition> listFunctions();

    Collection<FunctionDefinition> listFunctions(String pattern);

}
