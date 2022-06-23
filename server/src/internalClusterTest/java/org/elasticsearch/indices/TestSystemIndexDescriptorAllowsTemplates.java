/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A special kind of {@link SystemIndexDescriptor} that can toggle what kind of mappings it
 * expects. A real descriptor is immutable.
 */
public class TestSystemIndexDescriptorAllowsTemplates extends TestSystemIndexDescriptor {

    public static final String INDEX_NAME = ".templates-index";
    public static final String PRIMARY_INDEX_NAME = INDEX_NAME + "-1";

    public static final AtomicBoolean useNewMappings = new AtomicBoolean(false);

    TestSystemIndexDescriptorAllowsTemplates() {
        super(INDEX_NAME, PRIMARY_INDEX_NAME, true);
    }

    @Override
    public boolean isAutomaticallyManaged() {
        return true;
    }

    @Override
    public String getMappings() {
        return useNewMappings.get() ? getNewMappings() : getOldMappings();
    }
}
