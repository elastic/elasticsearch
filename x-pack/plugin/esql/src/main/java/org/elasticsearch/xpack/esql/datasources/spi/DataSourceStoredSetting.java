/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Objects;

/**
 * A validated datasource or dataset setting value paired with its sensitivity classification.
 * Returned by {@link DataSourceValidator} after validation. This is the representation
 * stored in cluster state as part of datasource and dataset metadata.
 *
 * <p>Secret values are masked on GET responses and encrypted when the encryption
 * layer is available. Values preserve their original JSON type (String, Integer,
 * Boolean, etc.) for non-lossy REST API round-trips.
 *
 * <p>This class will move to {@code server/} and implement {@code Writeable}
 * and {@code ToXContentObject} when the cluster state metadata layer lands.
 */
// Intentionally a class, not a record — will move to server/ and implement Writeable,
// which requires a StreamInput constructor that records cannot provide.
public final class DataSourceStoredSetting {

    private final Object value;
    private final boolean secret;

    public DataSourceStoredSetting(Object value, boolean secret) {
        this.value = value;
        this.secret = secret;
    }

    public Object value() {
        return value;
    }

    public boolean secret() {
        return secret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceStoredSetting that = (DataSourceStoredSetting) o;
        return secret == that.secret && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, secret);
    }

    @Override
    public String toString() {
        return "DataSourceStoredSetting{value=" + (secret ? "***" : value) + ", secret=" + secret + "}";
    }
}
