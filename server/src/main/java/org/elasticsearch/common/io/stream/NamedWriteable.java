/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

/**
 * A {@link Writeable} object identified by its name.
 * To be used for arbitrary serializable objects (e.g. queries); when reading them, their name tells
 * which specific object needs to be created.
 */
public interface NamedWriteable extends Writeable {

    /**
     * Returns the name of the writeable object
     */
    String getWriteableName();

    /**
     * The name {@link Symbol} used for efficient de-/serialization.
     * <p>
     * It's recommended to overwrite this default implementation to avoid unnecessary lookup costs.
     */
    default Symbol getNameSymbol() {
        return Symbol.ofConstant(getWriteableName());
    }
}
