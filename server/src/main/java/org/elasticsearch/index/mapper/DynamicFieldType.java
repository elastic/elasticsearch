/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

/**
 * Defines a MappedFieldType that exposes dynamic child field types
 *
 * If the field is named 'my_field', then a user is able to search on
 * the field in both of the following ways:
 *  - Using the field name 'my_field', which will delegate to the field type
 *    as usual.
 *  - Using any sub-key, for example 'my_field.some_key'. In this case, the
 *    search is delegated to {@link #getChildFieldType(String)}, with 'some_key'
 *    passed as the argument. The field may create a new field type dynamically
 *    in order to handle the search.
 *
 *  To prevent conflicts between these dynamic sub-keys and multi-fields, any
 *  field mappers generating field types that implement this interface should
 *  explicitly disallow multi-fields.
 */
public interface DynamicFieldType {

    /**
     * Returns a dynamic MappedFieldType for the given path
     */
    MappedFieldType getChildFieldType(String path);
}
