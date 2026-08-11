/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;

/**
 * The remote transport a catalog {@link SourceVariant} is read over. {@code S3} is preferred wherever
 * the upstream publisher exposes it; {@code HTTPS} is used only where no usable public S3 object exists.
 * {@code GCS} and {@code AZURE} are modelled now so a future public GCS/Azure source needs no catalog or
 * runner changes, even though every variant currently checked in uses {@code S3} or {@code HTTPS}
 * (elastic/esql-planning#1650 scopes AWS first).
 */
public enum PublicDataProvider {
    S3,
    HTTPS,
    GCS,
    AZURE;

    /** The {@code data_source} {@code type} passed to {@code PUT /_query/data_source} for this provider. */
    public String dataSourceType() {
        return switch (this) {
            case S3 -> "s3";
            case HTTPS -> "http";
            case GCS -> "gcs";
            case AZURE -> "azure";
        };
    }

    public static PublicDataProvider parse(String value) {
        return PublicDataProvider.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
