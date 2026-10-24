/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Per-batch field name cache that canonicalizes UTF-8 field name bytes into interned
 * {@link String} instances, then freezes into a compact open-addressing hash table for
 * fast repeated lookups during document walking.
 *
 * <h2>How it works</h2>
 *
 * <ol>
 *   <li><b>Parent / child model.</b> {@link FrozenFieldNameTable} is the shared root.
 *       Each parsing thread gets a thread-confined {@link FrozenFieldNameTable.Child}
 *       via {@link FrozenFieldNameTable#makeChild()} that implements
 *       {@link FieldNameLookup}.</li>
 *   <li><b>Hash + scan.</b> {@link FieldNameHash} computes a wyhash of field name bytes
 *       ({@link FieldNameHash#hashName}) or scans to the closing quote and hashes in one
 *       pass ({@link FieldNameHash#scanAndHash}). The walker passes the hash into lookup
 *       so bytes are not re-read on cache hits.</li>
 *   <li><b>Learning phase (first document).</b> On {@link FieldNameLookup#lookup} miss,
 *       the walker decodes UTF-8 and calls {@link FieldNameLookup#insert}. The child
 *       stores names in growable parallel arrays and resolves hits by linear scan.</li>
 *   <li><b>Freeze.</b> After the first document, {@link FieldNameLookup#freeze} builds a
 *       power-of-two open-addressing table keyed by hash, with an 8-byte prefix for fast
 *       rejection before full byte comparison. The frozen table is published to the
 *       parent so other threads can adopt it.</li>
 *   <li><b>Batch boundary.</b> {@link FieldNameLookup#release} merges any remaining
 *       discoveries to the parent and reuses the shared frozen table on the next batch.
 *       {@link org.elasticsearch.simdjson.SimdJsonDirectWalker#releaseNames()} triggers
 *       this at partition boundaries.</li>
 * </ol>
 *
 * <p>Instances are not thread-safe; one child per parsing thread.
 */
package org.elasticsearch.simdjson.internal.fieldnames;
