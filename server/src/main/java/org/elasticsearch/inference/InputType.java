/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import java.util.Locale;

/**
 * Defines the type of request, whether the request is to ingest a document or search for a document.
 */
public enum InputType {
    INGEST,
    SEARCH,
    UNSPECIFIED,
    CLASSIFICATION,
    CLUSTERING;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static InputType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
