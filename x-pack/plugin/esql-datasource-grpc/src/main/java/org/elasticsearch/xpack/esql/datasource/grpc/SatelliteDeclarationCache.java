/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * This node's copy of the capability declarations that satellites answered, so that a query does not repeat the capability round
 * trip of the handshake (protocol section 5.2), which is a cost per query on a remote endpoint.
 * <p>
 * A copy is current as long as the satellite says so. A satellite that gives a declaration id (the {@code declarationId} of its
 * declaration, repeated in the schema it answers {@code GetSchema} with, a call ES makes at every resolution anyway) is asked
 * nothing more while the id is the one the copy was kept under, and the copy is read again the moment the id changes, so a node
 * never plans from a declaration older than its resolution. The id is trusted for a bounded time ({@code ID_TRUST_FACTOR} times the
 * time to live, an hour by default), after which the copy is read again whatever the id says: a satellite whose id misses an input
 * of its declaration cannot keep a node on a stale copy for ever. A server that gives no id (a plain Flight server, or a satellite
 * of an earlier draft) gets a copy kept for the time to live, which then bounds how long a satellite that lost a capability can
 * keep failing the queries planned here and how long one that gained a capability goes unused. Only the declaration is kept; what
 * depends on the table as it is now (schema, record count) is read on every resolution.
 */
final class SatelliteDeclarationCache {

    /** The metadata entry of the table schema that carries the declaration id (protocol section 5.2). */
    static final String SCHEMA_METADATA_KEY = "esql.satellite.declaration_id";

    /** How many times the time to live a satellite's id is trusted for. */
    static final int ID_TRUST_FACTOR = 60;

    /** A declaration is at most 64 KiB of JSON, and parsed it takes several times that: a few entries per node are not many to keep. */
    static final int MAX_ENTRIES = 256;

    private static final String DECLARATION_PROPERTY = "declarationId";
    private static final Pattern VALID_ID = Pattern.compile("[!-~]{1,128}");

    private record Key(String endpoint, String target) {}

    /**
     * {@code declaration} is null for a server without one (a plain Flight server, or a satellite that shares no spec version with
     * ES): that answer is kept too. {@code id} is the id of the state this copy is of, null without one; {@code expiresAtNanos} ends
     * the time to live and {@code trustedUntilNanos} the time the id is trusted for.
     */
    private record Entry(Map<String, Object> declaration, @Nullable String id, long expiresAtNanos, long trustedUntilNanos) {}

    private final long ttlNanos;
    private final long trustNanos;
    private final LongSupplier nanoTime;
    private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();

    /** A zero time to live turns the cache off. */
    SatelliteDeclarationCache(TimeValue ttl) {
        this(ttl, System::nanoTime);
    }

    SatelliteDeclarationCache(TimeValue ttl, LongSupplier nanoTime) {
        this.ttlNanos = ttl.nanos();
        this.trustNanos = ttlNanos > Long.MAX_VALUE / (2 * ID_TRUST_FACTOR) ? Long.MAX_VALUE / 2 : ttlNanos * ID_TRUST_FACTOR;
        this.nanoTime = nanoTime;
    }

    boolean enabled() {
        return ttlNanos > 0;
    }

    /** The id the protocol allows: 1 to 128 ASCII graphic characters. Anything else is taken as no id at all. */
    @Nullable
    static String validId(@Nullable String id) {
        return id != null && VALID_ID.matcher(id).matches() ? id : null;
    }

    /**
     * The declaration of {@code target} on {@code endpoint}: the copy kept here if it is current, or what {@code fetch} reads and is
     * then kept. {@code observedId} is the id the satellite just gave in its {@code GetSchema} answer (null when it gave none or an
     * invalid one). When the satellite gives an id, the copy is current only if it was kept under that same id, and within the time
     * the id is trusted for: a copy kept without one, or under another, is of another state. When it gives none, the copy is current
     * for the time to live. A failed fetch is not kept. The result is a copy that the caller may add to, and is null for a server
     * without a declaration.
     */
    Map<String, Object> resolve(String endpoint, String target, @Nullable String observedId, Supplier<Map<String, Object>> fetch) {
        if (enabled() == false) {
            return fetch.get();
        }
        Key key = new Key(endpoint, target);
        Entry entry = entries.get(key);
        if (entry != null && isCurrent(entry, observedId)) {
            return copy(entry.declaration());
        }
        Map<String, Object> declaration = fetch.get();
        if (entries.size() >= MAX_ENTRIES && entries.containsKey(key) == false) {
            evictOldest();
        }
        // kept under the id the answer itself carries, never under the one seen before the call: if the satellite changed in
        // between, the next resolution sees an id that differs from this one and reads the declaration again. A server with no
        // declaration has no id of its own to carry, so the one it gave in its schema is the only one there is.
        String id = declaration == null ? observedId : idOf(declaration);
        long now = nanoTime.getAsLong();
        entries.put(key, new Entry(declaration, id, now + ttlNanos, now + trustNanos));
        return copy(declaration);
    }

    private boolean isCurrent(Entry entry, @Nullable String observedId) {
        long now = nanoTime.getAsLong();
        if (observedId != null) {
            // the satellite says which state it is in: a copy kept without an id, or under another one, is of another state
            return observedId.equals(entry.id()) && now - entry.trustedUntilNanos() < 0;
        }
        return now - entry.expiresAtNanos() < 0;
    }

    private void evictOldest() {
        entries.entrySet()
            .stream()
            .min((a, b) -> Long.compare(a.getValue().trustedUntilNanos(), b.getValue().trustedUntilNanos()))
            .ifPresent(oldest -> entries.remove(oldest.getKey(), oldest.getValue()));
    }

    @Nullable
    private static String idOf(@Nullable Map<String, Object> declaration) {
        return declaration != null && declaration.get(DECLARATION_PROPERTY) instanceof String id ? validId(id) : null;
    }

    /** Forgets the declaration of {@code target} on {@code endpoint}, so that the next resolution reads it again. */
    void invalidate(String endpoint, String target) {
        entries.remove(new Key(endpoint, target));
    }

    private static Map<String, Object> copy(Map<String, Object> declaration) {
        return declaration == null ? null : new HashMap<>(declaration);
    }
}
