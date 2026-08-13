/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Jackson (de)serialization of the flakiness contracts. Jackson is already on the {@code build-tools-internal}
 * classpath and natively supports records, so the contract records map 1:1 to their JSON with no custom
 * codecs.
 */
public final class FlakinessJson {

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private FlakinessJson() {}

    /** The {@code flakiness-refs.json} envelope (contract 1). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RefsFile(String mergeBase, List<FlakinessRef> refs) {}

    /**
     * The {@code flakiness-base-targets.json} envelope - the resolve-&gt;compile/scan hand-off. Carries the
     * resolved {@link BaseTarget}s (which the compile step turns into compile task paths and the scan step
     * scans) and the {@code unresolved} refs (folded verbatim into the plan by the scan step).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BaseTargetsFile(List<BaseTarget> targets, List<FlakinessPlan.Unresolved> unresolved) {}

    public static RefsFile parseRefs(String json) {
        try {
            return MAPPER.readValue(json, RefsFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse flakiness-refs.json", e);
        }
    }

    public static String writeBaseTargetsFile(BaseTargetsFile file) {
        try {
            return MAPPER.writeValueAsString(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize flakiness-base-targets.json", e);
        }
    }

    public static BaseTargetsFile parseBaseTargetsFile(String json) {
        try {
            return MAPPER.readValue(json, BaseTargetsFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse flakiness-base-targets.json", e);
        }
    }

    public static String writePlan(FlakinessPlan plan) {
        try {
            return MAPPER.writeValueAsString(plan);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize flakiness-plan.json", e);
        }
    }
}
