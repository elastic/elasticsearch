/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.capabilities;


public interface Unresolvable extends Resolvable {

    String UNRESOLVED_PREFIX = "?";

    @Override
    default boolean resolved() {
        return false;
    }

    String unresolvedMessage();
}
