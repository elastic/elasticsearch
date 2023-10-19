/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import java.util.List;

public class SystemIndexDescriptorUtils {
    /**
     * Creates a descriptor for system indices matching the supplied pattern. These indices will not be managed
     * by Elasticsearch internally.
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character, must not
     *                     overlap with any other descriptor patterns, and must allow a suffix (see note on
     *                     {@link SystemIndexDescriptor} for details).
     * @param description The name of the plugin responsible for this system index.
     */
    public static SystemIndexDescriptor createUnmanaged(String indexPattern, String description) {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(indexPattern)
            .setDescription(description)
            .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
            .setAllowedElasticProductOrigins(List.of())
            .build();
    }
}
