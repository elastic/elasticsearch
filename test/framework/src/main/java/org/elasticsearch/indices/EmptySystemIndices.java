/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import java.util.List;

/**
 * A test class which contains a singleton instance of the {@link SystemIndices} class that has been created with no
 * non-standard system indices defined except for those defined within the class itself.
 */
public class EmptySystemIndices extends SystemIndices {

    public static final SystemIndices INSTANCE = new EmptySystemIndices();

    private EmptySystemIndices() {
        super(List.of());
    }
}
