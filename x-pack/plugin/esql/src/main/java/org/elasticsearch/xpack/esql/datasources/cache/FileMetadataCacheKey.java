/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

/**
 * Key for a cached {@link FileMetadata}. Deliberately credential-INDEPENDENT — a storage identity names only
 * the fields its configuration declares non-secret — so the entry is shared across users exactly like the
 * schema cache: the same canonical path under the same storage identity resolves to the same object
 * regardless of who asks. The same canonical path under a different storage identity may resolve to a
 * different object, which is why the storage identity is part of the key and the credential is not.
 *
 * @param storageIdentity what the storage provider that would read this object says identifies it. Passed in
 *                        rather than read out of a config map, because only that provider knows which of its
 *                        settings name the same object twice. Two literals chosen here would name nothing for a
 *                        provider addressed by an account rather than an endpoint.
 */
public record FileMetadataCacheKey(String canonicalPath, String storageIdentity) {}
