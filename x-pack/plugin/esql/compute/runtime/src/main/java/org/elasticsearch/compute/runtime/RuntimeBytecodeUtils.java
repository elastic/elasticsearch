/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

/**
 * Shared constants for runtime bytecode generation.
 * Contains type descriptors for classes used across evaluator implementations.
 */
final class RuntimeBytecodeUtils {

    private RuntimeBytecodeUtils() {}

    // Type descriptors for classes that may not be on the compile classpath
    static final String SOURCE_CLASS_NAME = "org.elasticsearch.xpack.esql.core.tree.Source";
    static final String SOURCE_INTERNAL = "org/elasticsearch/xpack/esql/core/tree/Source";
    static final String WARNINGS_MODE_INTERNAL = "org/elasticsearch/compute/operator/DriverContext$WarningsMode";
    static final String WARNING_SOURCE_LOCATION_INTERNAL = "org/elasticsearch/compute/operator/WarningSourceLocation";
}
