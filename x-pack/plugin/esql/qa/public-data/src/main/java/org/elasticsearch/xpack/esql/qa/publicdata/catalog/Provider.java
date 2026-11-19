/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.List;
import java.util.Locale;

/**
 * Object-store providers the suite models. Only {@link #S3} is <em>active</em>: HTTPS variants are
 * inert backup catalog entries, and GCS/Azure are declared gaps until the {@code type: gcs}/'
 * {@code type: azure} data-source definitions become usable. All four are modelled so activating a
 * provider later is a catalog edit, not a redesign.
 */
public enum Provider {
    /** Amazon S3, anonymous access only. The sole active provider. */
    S3("s3", "s3", true, true, List.of("s3://")),
    /**
     * Plain HTTPS. Cannot list directories, so glob/multi-file layouts are structurally impossible
     * (a first-class {@code blocked} cell, not a skip). Backup entries only.
     */
    HTTPS("https", "http", false, false, List.of("https://")),
    /** Google Cloud Storage. Modelled, but a declared gap until data-source support lands. */
    GCS("gcs", "gcs", true, false, List.of("gs://")),
    /** Azure Blob Storage. Modelled, but a declared gap until data-source support lands. */
    AZURE("azure", "azure", true, false, List.of("wasbs://", "abfss://", "azure://"));

    private final String id;
    private final String esType;
    private final boolean supportsGlob;
    private final boolean active;
    private final List<String> schemes;

    Provider(String id, String esType, boolean supportsGlob, boolean active, List<String> schemes) {
        this.id = id;
        this.esType = esType;
        this.supportsGlob = supportsGlob;
        this.active = active;
        this.schemes = schemes;
    }

    /** The identifier used in {@code public-data-catalog.yml} and in variant labels. */
    public String id() {
        return id;
    }

    /** The {@code data_source} {@code type} to register against the cluster. */
    public String esType() {
        return esType;
    }

    /** Whether the provider can list objects, i.e. whether glob/multi-file layouts are possible. */
    public boolean supportsGlob() {
        return supportsGlob;
    }

    /** Whether the provider participates in active runs (vs. backup/gap modelling). */
    public boolean active() {
        return active;
    }

    /** Whether {@code resource} carries one of this provider's URI schemes. */
    public boolean matchesScheme(String resource) {
        return schemes.stream().anyMatch(resource::startsWith);
    }

    public static Provider fromId(String id) {
        for (Provider provider : values()) {
            if (provider.id.equals(id.toLowerCase(Locale.ROOT))) {
                return provider;
            }
        }
        throw new IllegalArgumentException("Unknown provider [" + id + "]; expected one of s3, https, gcs, azure");
    }
}
