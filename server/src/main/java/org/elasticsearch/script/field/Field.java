/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

/** A field in a document accessible via scripting. */
public interface Field<T> extends Iterable<T> {

    /** Returns the name of this field. */
    String getName();

    /** Returns {@code true} if this field has no values, otherwise {@code false}. */
    boolean isEmpty();

    /** Returns the number of values this field has. */
    int size();
}
