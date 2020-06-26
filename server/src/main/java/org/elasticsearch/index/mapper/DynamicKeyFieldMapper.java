/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;

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
 * Note: we anticipate that 'flattened' fields will be the only implementation of this
 * interface. Flattened object fields live in the 'mapper-flattened' module.
 */
public abstract class DynamicKeyFieldMapper extends FieldMapper {

    public DynamicKeyFieldMapper(String simpleName,
                                 FieldType fieldType,
                                 MappedFieldType defaultFieldType,
                                 CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, MultiFields.empty(), copyTo);
    }

    public abstract MappedFieldType keyedFieldType(String key);

}
