/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;

/**
 * Configuration for extracting a single sub-key from a {@code flattened} field root,
 * enabling {@code field_extract(root, "host.name")} to fuse into the keyed sub-field
 * doc-values block loader instead of materializing the whole flattened JSON per row.
 * The {@code key} is the literal flattened-storage key, exactly the dotted name as it
 * appears in doc values for the flattened root (e.g. {@code "host.name"}).
 */
public record ExtractFlattenedSubfieldConfig(String key) implements BlockLoaderFunctionConfig {
    @Override
    public Function function() {
        return Function.EXTRACT_FLATTENED_SUBFIELD;
    }
}
