/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * How a {@link SourceVariant}'s {@link PinInfo} is re-verified before the suite queries it. Most upstream
 * publishers expose genuinely immutable objects, for which a plain HTTP {@code ETag}/{@code Content-Length}
 * check is both sufficient and metadata-only (never a body fetch). A few publishers, however, wholesale
 * regenerate an object on a schedule (e.g. a "historical year" export rewritten daily even though its
 * content is unchanged) so its transport-level {@code ETag}/{@code Last-Modified} drift constantly while
 * the actual rows do not; for those, {@link #ETAG} would fail every single run for no real reason.
 */
public enum PinStrategy {

    /**
     * Default. {@link org.elasticsearch.xpack.esql.qa.publicdata.PinValidator} re-issues a {@code HEAD}
     * and fails loudly if the live {@code ETag}/{@code Content-Length} no longer match {@link PinInfo}.
     */
    ETAG,

    /**
     * The upstream object's transport metadata is known to drift independently of its content (see class
     * Javadoc). {@link org.elasticsearch.xpack.esql.qa.publicdata.PinValidator} still confirms the object
     * is reachable, but does not fail on an {@code ETag}/{@code Content-Length} mismatch; instead,
     * {@link PinInfo#contentSignature()} documents a small content fingerprint (e.g. row count plus a
     * couple of aggregate values) computed once, at authoring time, via a bounded (&lt;500MB), in-memory,
     * never-persisted DuckDB/ClickHouse read of the object -- the same bounded read used to independently
     * cross-validate the checked-in expected results. That fingerprint is documentation-only: re-verifying
     * it live on every run would require the same body fetch {@link org.elasticsearch.xpack.esql.qa.publicdata.PinValidator}
     * otherwise never performs, so a maintainer re-running the authoring-time DuckDB/ClickHouse check by
     * hand is what re-establishes it, not the automated suite.
     */
    CONTENT_SIGNATURE;

    public static PinStrategy parse(String value) {
        return PinStrategy.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
