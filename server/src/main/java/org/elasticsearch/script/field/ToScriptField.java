/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.Supplier;

/**
 * The scripting fields API requires knowledge of the {@link MappedFieldType} when a user
 * accesses a field in a script to return an appropriate {@link DocValuesField}. However,
 * the {@link MappedFieldType} is not directly known to the {@link LeafFieldData}
 * the values are loaded into. This functional interface is used to return a
 * {@link DocValuesField} based on the {@link MappedFieldType} created by the
 * {@link MappedFieldType#fielddataBuilder(String, Supplier)}.
 *
 * @param <T> The type of doc values data passed into the constructor for a {@link DocValuesField}.
 */
public interface ToScriptField<T> {

    DocValuesField<?> getScriptField(T docValues, String name);
}
