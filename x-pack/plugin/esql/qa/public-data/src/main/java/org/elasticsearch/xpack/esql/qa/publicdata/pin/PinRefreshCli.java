/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Entry point for the {@code refreshPublicDataPins} Gradle task: prints refreshed {@code pin:}
 * YAML blocks from live metadata to stdout for the operator to paste into the catalog. With
 * {@code --write <catalog.yml>} it splices the refreshed blocks into the file in place — the
 * mechanical half of the PIN_DRIFT maintenance flow. Either way a human stays in the loop by
 * construction: a pin refresh means the upstream bytes changed, so the re-derive/re-review
 * workflow (and a reviewed diff) must follow before anything ships.
 */
public final class PinRefreshCli {

    /** How many per-object samples a refreshed LIST pin records (first and last keys). */
    private static final int LIST_SAMPLES = 6;

    private PinRefreshCli() {}

    @SuppressForbidden(reason = "CLI tool prints refreshed pin YAML to stdout for the operator")
    public static void main(String[] args) throws IOException {
        java.nio.file.Path writeTarget = null;
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--write")) {
                writeTarget = java.nio.file.Path.of(args[i + 1]);
            }
        }
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        java.util.Map<String, String> refreshed = new java.util.LinkedHashMap<>();
        for (var corpus : catalog.corpora()) {
            for (VariantSpec variant : corpus.variants()) {
                if (variant.active() == false) {
                    continue;
                }
                System.out.println("# variant: " + variant.label());
                try {
                    String pinYaml = renderPin(variant);
                    System.out.println(pinYaml);
                    refreshed.put(variant.resource(), pinYaml);
                } catch (UnsupportedOperationException | IOException e) {
                    System.out.println("#   (unrefreshable: " + e.getMessage() + ")");
                }
            }
        }
        if (writeTarget != null) {
            String updated = spliceRefreshedPins(java.nio.file.Files.readString(writeTarget), refreshed);
            java.nio.file.Files.writeString(writeTarget, updated);
            System.out.println("# wrote refreshed pins into " + writeTarget + " (review the diff before shipping)");
        }
    }

    /**
     * Splices refreshed {@code pin:} blocks into the catalog text, keyed by each variant's
     * {@code resource:} line. Text surgery, not YAML re-serialization: the catalog's comments and
     * ordering are load-bearing documentation and must survive a refresh. A pin block is the
     * {@code pin:} line following a matched resource (before the next variant/corpus) plus its
     * more-indented body lines. Package-private for tests.
     */
    static String spliceRefreshedPins(String catalogText, java.util.Map<String, String> refreshedByResource) {
        java.util.List<String> lines = new java.util.ArrayList<>(catalogText.lines().toList());
        for (var entry : refreshedByResource.entrySet()) {
            String resourceLine = "resource: \"" + entry.getKey() + "\"";
            int resourceAt = -1;
            for (int i = 0; i < lines.size(); i++) {
                if (lines.get(i).trim().equals(resourceLine)) {
                    resourceAt = i;
                    break;
                }
            }
            if (resourceAt < 0) {
                continue;
            }
            int pinAt = -1;
            for (int i = resourceAt + 1; i < lines.size(); i++) {
                String trimmed = lines.get(i).trim();
                if (trimmed.equals("pin:")) {
                    pinAt = i;
                    break;
                }
                if (trimmed.startsWith("- provider:") || trimmed.startsWith("- id:") || trimmed.equals("gaps:")) {
                    break; // next variant/corpus: this one has no pin block
                }
            }
            if (pinAt < 0) {
                continue;
            }
            int pinIndent = indentOf(lines.get(pinAt));
            int end = pinAt + 1;
            while (end < lines.size() && (lines.get(end).isBlank() || indentOf(lines.get(end)) > pinIndent)) {
                end++;
            }
            String prefix = " ".repeat(pinIndent);
            java.util.List<String> replacement = entry.getValue().lines().map(l -> l.isBlank() ? l : prefix + l).toList();
            lines.subList(pinAt, end).clear();
            lines.addAll(pinAt, replacement);
        }
        return String.join("\n", lines) + "\n";
    }

    private static int indentOf(String line) {
        int i = 0;
        while (i < line.length() && line.charAt(i) == ' ') {
            i++;
        }
        return i;
    }

    private static String renderPin(VariantSpec variant) throws IOException {
        PinProbe probe = PinVerifier.defaultProbe(variant);
        boolean listPin = variant.pin() != null && "LIST".equalsIgnoreCase(variant.pin().method());
        if (listPin == false && variant.pin() == null) {
            // no existing pin: infer from the resource shape
            listPin = variant.resource().matches(".*[*?\\[{].*");
        }
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        StringBuilder yaml = new StringBuilder("pin:\n");
        if (listPin) {
            List<ObjectMetadata> live = probe.list(PinVerifier.listPrefix(variant.resource()), 100_000);
            long totalBytes = live.stream().mapToLong(ObjectMetadata::sizeBytes).sum();
            yaml.append("  method: LIST\n");
            yaml.append("  verified_at: ").append(now).append('\n');
            yaml.append("  object_count: ").append(live.size()).append('\n');
            yaml.append("  total_bytes: ").append(totalBytes).append('\n');
            yaml.append("  samples:\n");
            for (ObjectMetadata sample : sampleOf(live)) {
                appendSample(yaml, new PinSpec.PinnedObject(sample.key(), sample.etag(), sample.sizeBytes()));
            }
        } else {
            ObjectMetadata live = probe.head(variant.resource());
            yaml.append("  method: HEAD\n");
            yaml.append("  verified_at: ").append(now).append('\n');
            yaml.append("  object_count: 1\n");
            yaml.append("  total_bytes: ").append(live.sizeBytes()).append('\n');
            yaml.append("  samples:\n");
            appendSample(yaml, new PinSpec.PinnedObject(live.key(), live.etag(), live.sizeBytes()));
        }
        return yaml.toString();
    }

    private static void appendSample(StringBuilder yaml, PinSpec.PinnedObject sample) {
        yaml.append("    - key: \"").append(sample.key()).append("\"\n");
        if (sample.etag() != null) {
            yaml.append("      etag: \"").append(sample.etag()).append("\"\n");
        }
        yaml.append("      size: ").append(sample.sizeBytes()).append('\n');
    }

    private static List<ObjectMetadata> sampleOf(List<ObjectMetadata> live) {
        if (live.size() <= LIST_SAMPLES) {
            return live;
        }
        int half = LIST_SAMPLES / 2;
        var samples = new java.util.ArrayList<>(live.subList(0, half));
        samples.addAll(live.subList(live.size() - half, live.size()));
        return samples;
    }
}
