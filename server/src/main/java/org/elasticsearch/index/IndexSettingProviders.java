/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import java.util.Collections;
import java.util.Set;

/**
 * Keeps track of the {@link IndexSettingProvider} instances defined by plugins and
 * this class can be used by other components to get access to {@link IndexSettingProvider} instances.
 */
public final class IndexSettingProviders {

    private final Set<IndexSettingProvider> indexSettingProviders;

    public IndexSettingProviders(Set<IndexSettingProvider> indexSettingProviders) {
        this.indexSettingProviders = Collections.unmodifiableSet(indexSettingProviders);
    }

    public Set<IndexSettingProvider> getIndexSettingProviders() {
        return indexSettingProviders;
    }
}
