/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Sometimes we release a version with a bunch of cool new features, and we want people to be able to start a new trial license in a cluster
 * that's already used a trial and let it expire. This class controls when we do that. The serialization of this class is designed to
 * maintain compatibility with old-school Elasticsearch versions (specifically the {@link org.elasticsearch.Version} class).
 */
public class TrialLicenseEra implements ToXContentFragment, Writeable {

    // Increment this when we want users to be able to start a new trial. Note that BWC with old versions of Elasticsearch cause this to be
    // limited to a maximum of 99 (inclusive). See the Version class for more info.
    public static final TrialLicenseEra CURRENT = new TrialLicenseEra(8);

    private final int era;

    public TrialLicenseEra(int era) {
        this.era = era;
    }

    public TrialLicenseEra(StreamInput in) throws IOException {
        this.era = (in.readVInt() - 99) / 1000000; // Copied from the constructor of Version
    }

    public static TrialLicenseEra fromString(String from) {
        String[] parts = from.split("[.-]");
        if (parts.length == 0) {
            throw new IllegalArgumentException("illegal trial era format: [" + from + "]");
        }
        try {
            final int era = Integer.parseInt(parts[0]);
            return new TrialLicenseEra(era);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse trial era: [" + from + "]", e);
        }
    }

    int asInt() {
        return era;
    }

    public boolean ableToStartNewTrialSince(TrialLicenseEra since) {
        return since.asInt() < era;
    }

    @Override
    public String toString() {
        return Integer.toString(era);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(asVersionString()); // suffix added for BWC
    }

    // pkg-private for testing
    String asVersionString() {
        return this + ".0.0";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrialLicenseEra that = (TrialLicenseEra) o;
        return era == that.era;
    }

    @Override
    public int hashCode() {
        return Objects.hash(era);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt((era * 1000000) + 99); // matches Version serialization
    }
}
