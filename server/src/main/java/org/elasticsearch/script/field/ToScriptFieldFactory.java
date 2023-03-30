/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.LeafFieldData;

/**
 * A helper class that lets {@link LeafFieldData#getScriptFieldFactory} translate from raw doc values
 * into the field provider that the scripting API requires. It can make use of information specific
 * to the mapped field type, in a way {@link LeafFieldData} is not designed to do.
 *
 * @param <T> The type of doc values data used to construct a {@link DocValuesScriptFieldFactory}.
 */
public interface ToScriptFieldFactory<T> {

    DocValuesScriptFieldFactory getScriptFieldFactory(T docValues, String name);
}
