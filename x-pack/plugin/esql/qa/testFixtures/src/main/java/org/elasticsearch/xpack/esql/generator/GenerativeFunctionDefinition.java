/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.List;

/**
 * An ES|QL function definition loaded from the generated JSON docs under
 * {@code docs/reference/query-languages/esql/kibana/generated/x-pack-esql/definition/functions/}.
 */
public record GenerativeFunctionDefinition(
    String name,
    String functionType,
    List<GenerativeFunctionSignature> signatures,
    boolean snapshotOnly,
    boolean preview
) {}
