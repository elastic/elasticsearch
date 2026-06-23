/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.cluster.metadata.IndexAbstraction;

import java.util.EnumSet;

/**
 * Per-kind schema producer behind the {@code resolve_schema} umbrella. One provider per kind of index abstraction
 * (indices, views, datasets); the umbrella dispatches the schema step to the provider that {@link #handles} the kind.
 * This is the open extension point that keeps the unified schema-discovery action from becoming the closed monolith
 * field-caps was: a new abstraction kind is a new provider, not an edit to a central switch.
 */
public interface AbstractionSchemaProvider {

    /** The index-abstraction kinds this provider resolves schemas for. */
    EnumSet<IndexAbstraction.Type> handles();
}
