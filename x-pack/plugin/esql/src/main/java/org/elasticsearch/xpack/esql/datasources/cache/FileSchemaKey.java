/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

/**
 * The address of one file's inferred SCHEMA.
 * <p>
 * Three components and no discriminator. A schema describes the file itself, so it is the same answer
 * whoever asks and needs nothing about the read that asked: what the file is ({@code canonicalPath}),
 * which version of it ({@code lastModifiedEpochMillis} - a changed file derives a different address and
 * the stale one ages out by itself, so there is no invalidation protocol), and whose it is
 * ({@link SourceScope}, held by reference and so shared across every file of the dataset).
 * <p>
 * mtime in the address rather than beside it, because that is what makes the store correct-or-miss.
 */
public record FileSchemaKey(SourceScope scope, String canonicalPath, long lastModifiedEpochMillis) implements FileAddressed {}
