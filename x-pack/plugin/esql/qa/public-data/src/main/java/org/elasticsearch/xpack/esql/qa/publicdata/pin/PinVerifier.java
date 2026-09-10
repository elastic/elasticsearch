/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.Provider;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Verifies catalog pins against live object-store metadata — the scheduled pipeline's first step.
 * Any ETag/size/object-count change marks the variant {@code PIN_DRIFT}, so upstream re-publishes
 * surface as attributed maintenance (re-pin, re-derive, re-review) instead of masquerading as a
 * product regression. Strictly metadata-only: HTTP {@code HEAD} and S3 {@code ListObjectsV2}.
 */
public class PinVerifier {

    /** Verification outcome for one variant. */
    public enum Status {
        /** Live metadata matches the pin. */
        OK,
        /** Live metadata differs from the pin: the upstream bytes moved. */
        PIN_DRIFT,
        /** The store could not be reached after bounded retries; attributed as infrastructure. */
        UNREACHABLE,
        /** Backup/inert entry or missing pin; nothing verified. */
        SKIPPED
    }

    /** One variant's verification result. */
    public record VariantResult(String corpusId, String label, Status status, List<String> details) {}

    private final Function<VariantSpec, PinProbe> probeFactory;

    public PinVerifier() {
        this(PinVerifier::defaultProbe);
    }

    /** Probe factory injectable for tests (which must prove no body is ever fetched). */
    public PinVerifier(Function<VariantSpec, PinProbe> probeFactory) {
        this.probeFactory = probeFactory;
    }

    static PinProbe defaultProbe(VariantSpec variant) {
        if (variant.provider() == Provider.S3) {
            return new S3AnonymousPinProbe(variant.region());
        }
        if (variant.provider() == Provider.HTTPS) {
            return new HttpsPinProbe();
        }
        throw new UnsupportedOperationException("No pin probe for provider [" + variant.provider() + "] yet (declared gap)");
    }

    /** Verifies every active, pinned variant in the catalog. */
    public List<VariantResult> verify(PublicDataCatalog catalog) {
        List<VariantResult> results = new ArrayList<>();
        catalog.corpora().forEach(corpus -> corpus.variants().forEach(variant -> results.add(verifyVariant(variant))));
        return results;
    }

    public VariantResult verifyVariant(VariantSpec variant) {
        if (variant.active() == false || variant.pin() == null) {
            return new VariantResult(variant.corpusId(), variant.label(), Status.SKIPPED, List.of("backup/inert entry or no pin"));
        }
        try {
            List<String> drift = "LIST".equalsIgnoreCase(variant.pin().method()) ? verifyByList(variant) : verifyByHead(variant);
            return new VariantResult(
                variant.corpusId(),
                variant.label(),
                drift.isEmpty() ? Status.OK : Status.PIN_DRIFT,
                drift.isEmpty() ? List.of("metadata matches pin") : drift
            );
        } catch (IOException e) {
            return new VariantResult(variant.corpusId(), variant.label(), Status.UNREACHABLE, List.of(e.toString()));
        }
    }

    private List<String> verifyByHead(VariantSpec variant) throws IOException {
        PinProbe probe = probeFactory.apply(variant);
        PinSpec pin = variant.pin();
        List<String> drift = new ArrayList<>();
        for (PinSpec.PinnedObject pinned : pin.samples()) {
            ObjectMetadata live = probe.head(headUri(variant, pinned.key()));
            compare(pin, pinned, live, drift);
        }
        return drift;
    }

    private List<String> verifyByList(VariantSpec variant) throws IOException {
        PinProbe probe = probeFactory.apply(variant);
        PinSpec pin = variant.pin();
        List<String> drift = new ArrayList<>();
        List<ObjectMetadata> live = probe.list(listPrefix(variant.resource()), (int) Math.max(pin.objectCount() * 2, 1000));
        // Deliberately strict even for a volatile pin: a publisher that rewrites objects in place is
        // one thing, a prefix that gained or lost objects is a different corpus and must be reviewed.
        if (live.size() != pin.objectCount()) {
            drift.add("object count changed: pinned " + pin.objectCount() + ", live " + live.size());
        }
        long liveBytes = live.stream().mapToLong(ObjectMetadata::sizeBytes).sum();
        if (pin.isVolatile() ? pin.sizeWithinTolerance(pin.totalBytes(), liveBytes) == false : liveBytes != pin.totalBytes()) {
            drift.add("total bytes changed: pinned " + pin.totalBytes() + ", live " + liveBytes);
        }
        Map<String, ObjectMetadata> liveByKey = new java.util.HashMap<>();
        live.forEach(m -> liveByKey.put(m.key(), m));
        for (PinSpec.PinnedObject pinned : pin.samples()) {
            ObjectMetadata match = liveByKey.get(pinned.key());
            if (match == null) {
                drift.add("pinned object vanished: " + pinned.key());
            } else {
                compare(pin, pinned, match, drift);
            }
        }
        return drift;
    }

    /**
     * Frozen objects must match byte-for-byte identity; a volatile one only has to still be there at
     * roughly its pinned size. The ETag of a volatile object is deliberately not compared — the
     * publisher rewrites it on a schedule, and reporting that every night would train the reader to
     * ignore PIN_DRIFT, which is the one signal that must stay meaningful.
     */
    private static void compare(PinSpec pin, PinSpec.PinnedObject pinned, ObjectMetadata live, List<String> drift) {
        if (pin.isVolatile()) {
            if (pin.sizeWithinTolerance(pinned.sizeBytes(), live.sizeBytes()) == false) {
                drift.add(
                    pinned.key()
                        + " size moved beyond the volatile pin's "
                        + pin.sizeTolerancePercent()
                        + "% tolerance: pinned "
                        + pinned.sizeBytes()
                        + ", live "
                        + live.sizeBytes()
                );
            }
            return;
        }
        if (pinned.sizeBytes() != live.sizeBytes()) {
            drift.add(pinned.key() + " size changed: pinned " + pinned.sizeBytes() + ", live " + live.sizeBytes());
        }
        if (pinned.etag() != null && pinned.etag().equals(live.etag()) == false) {
            drift.add(pinned.key() + " etag changed: pinned " + pinned.etag() + ", live " + live.etag());
        }
    }

    /** Resolves the URI to {@code HEAD} for a pinned key: HTTPS pins record full URIs already. */
    private static String headUri(VariantSpec variant, String key) {
        if (variant.provider() == Provider.HTTPS) {
            return key.startsWith("https://") ? key : variant.resource();
        }
        S3AnonymousPinProbe.S3Location location = S3AnonymousPinProbe.S3Location.parse(variant.resource());
        return "s3://" + location.bucket() + "/" + key;
    }

    /**
     * The longest literal listing prefix of a resource: glob metacharacters are stripped, and a
     * comma-separated multi-location resource contributes the common prefix of its entries.
     */
    static String listPrefix(String resource) {
        String[] locations = resource.split(",");
        String prefix = locations[0];
        for (String location : locations) {
            int common = 0;
            while (common < prefix.length() && common < location.length() && prefix.charAt(common) == location.charAt(common)) {
                common++;
            }
            prefix = prefix.substring(0, common);
        }
        int firstMeta = prefix.length();
        for (char meta : new char[] { '*', '?', '[', '{' }) {
            int index = prefix.indexOf(meta);
            if (index >= 0 && index < firstMeta) {
                firstMeta = index;
            }
        }
        return prefix.substring(0, firstMeta);
    }
}
