/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.indices.AbstractSystemIndexFormatVersionTests;
import org.elasticsearch.indices.SystemIndexDescriptor;

import java.util.Collection;
import java.util.List;

public class AsyncSystemIndexTests extends AbstractSystemIndexFormatVersionTests {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        return List.of(AsyncTaskIndexService.getSystemIndexDescriptor());
    }
}
