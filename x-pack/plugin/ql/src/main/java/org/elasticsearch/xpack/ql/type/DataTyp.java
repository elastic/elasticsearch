/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

public interface DataTyp {

    /**
     * Type's name used for error messages and column info for the clients
     */
    String typeName();

    /**
     * Elasticsearch data type that it maps to
     */
    String esType();

    /**
     * Size of the type in bytes
     * <p>
     * -1 if the size can vary
     */
    int size();
}
