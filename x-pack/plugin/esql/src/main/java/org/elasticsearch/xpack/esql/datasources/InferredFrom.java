/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * The files a dataset's inferred schema was inferred from, identified precisely enough that a cached schema can be
 * reused exactly when they are unchanged.
 * <p>
 * Which files those are is a property of the discovery mode, not of the dataset, and {@link SchemaBreadth} decides it.
 * Under {@code first_file_wins} the schema is the anchor's alone, so a file appended after the anchor cannot change it
 * and must not invalidate it; under {@code union_by_name} and {@code strict} every file contributes, so any change to
 * the set must.
 * <p>
 * The two cases are distinct types rather than one shape with a flag, so an identity taken from one file can never
 * equal one taken from a whole listing, whatever their bits.
 */
public sealed interface InferredFrom {

    /** The one file that defines the schema: the head of the listing, in the order the listing was built. */
    record Anchor(FileFingerprint anchor) implements InferredFrom {}

    /** Every file in the listing, order-independent. */
    record EveryFile(FileSetFingerprint files) implements InferredFrom {}
}
