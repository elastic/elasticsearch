/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.analysis.NamedAnalyzer;

/**
 * A field mapper that supports lookup of dynamic sub-keys. If the field mapper is named 'my_field',
 * then a user is able to search on the field in both of the following ways:
 * - Using the field name 'my_field', which will delegate to the field type
 *   {@link DynamicKeyFieldMapper#fieldType()} as usual.
 * - Using any sub-key, for example 'my_field.some_key'. In this case, the search is delegated
 *   to {@link DynamicKeyFieldMapper#keyedFieldType(String)}, with 'some_key' passed as the
 *   argument. The field mapper is allowed to create a new field type dynamically in order
 *   to handle the search.
 *
 * To prevent conflicts between these dynamic sub-keys and multi-fields, any field mappers
 * implementing this interface should explicitly disallow multi-fields. The constructor makes
 * sure to passes an empty multi-fields list to help prevent conflicting sub-keys from being
 * registered.
 *
 * Note: currently 'flattened' fields are the only implementation of this interface.
 */
public abstract class DynamicKeyFieldMapper extends FieldMapper {

    public DynamicKeyFieldMapper(String simpleName,
                                 MappedFieldType defaultFieldType,
                                 NamedAnalyzer indexAnalyzer,
                                 CopyTo copyTo) {
        super(simpleName, defaultFieldType, indexAnalyzer, MultiFields.empty(), copyTo);
    }

    public abstract MappedFieldType keyedFieldType(String key);

}
