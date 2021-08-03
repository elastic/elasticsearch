/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */


package org.elasticsearch.script;

import java.util.List;

/**
 * A field in a document accessible via scripting.  In search contexts, the Field may be backed by doc values, source
 * or both.  In ingestion, the field may be in the source document or being added to the document.
 *
 * Field's methods must not throw exceptions nor return null.  A Field object representing a empty or unmapped field will have
 * * {@code isEmpty() == true}
 * * {@code getValues().equals(Collections.emptyList())}
 * * {@code getValue(defaultValue) == defaultValue}
 * @param <T>
 */
public interface Field<T> {
    String getName();
    /** Does the field have any values? An unmapped field may have values from source */
    boolean isEmpty();
    /** Get all values of a multivalued field.  If {@code isEmpty()} this returns an empty list */
    List<T> getValues();
    /** Get the first value of a field, if {@code isEmpty()} return defaultValue instead */
    T getValue(T defaultValue);
}
