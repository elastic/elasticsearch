/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

/**
 * A special kind of {@link SystemIndexDescriptor} that is exactly like {@link TestSystemIndexDescriptor},
 * but it allows for user defined templates to apply to it.
 */
public class TestSystemIndexDescriptorAllowsTemplates extends TestSystemIndexDescriptor {

    public static final String INDEX_NAME = ".templates-index";
    public static final String PRIMARY_INDEX_NAME = INDEX_NAME + "-1";

    TestSystemIndexDescriptorAllowsTemplates() {
        super(INDEX_NAME, PRIMARY_INDEX_NAME, true);
    }
}
