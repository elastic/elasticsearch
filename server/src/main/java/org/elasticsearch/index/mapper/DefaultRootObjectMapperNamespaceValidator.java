/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * No-op Default of RootObjectMapperNamespaceValidator used in non-serverless Elasticsearch envs.
 */
public class DefaultRootObjectMapperNamespaceValidator implements RootObjectMapperNamespaceValidator {
    @Override
    public void validateNamespace(ObjectMapper.Subobjects subobjects, Mapper mapper) {}

    // MP FIXME remove
    @Override
    public String toString() {
        return "I'm the DefaultRootObjectMapperNamespaceValidator{}";
    }
}
