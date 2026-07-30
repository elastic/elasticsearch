/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A coordinator-local carrier for the merged field-caps ES|QL already resolved for an {@code EQL} source pattern, so the
 * delegated EQL search can reuse it instead of issuing its own {@code _field_caps} request (see
 * {@code EqlRequests}/{@code TransportEqlSearchAction}). Never serialized — it rides the plan node
 * ({@code EqlRelation}/{@code EqlSourceExec}) in-memory and is consumed once at execution planning.
 *
 * <p>All instances compare equal and hash to a constant: the payload is execution metadata, not plan semantics, so it must
 * never affect plan equality or the analyzer's fixed-point checks. An empty carrier (or a consumed one) simply means the EQL
 * engine falls back to resolving field-caps itself.
 */
public final class PreResolvedFieldCaps {

    /** The empty carrier: nothing to inject, EQL resolves its own field-caps. Used when nothing was retained. */
    public static final PreResolvedFieldCaps NONE = new PreResolvedFieldCaps(null);

    private final AtomicReference<FieldCapabilitiesResponse> ref;

    public PreResolvedFieldCaps(@Nullable FieldCapabilitiesResponse caps) {
        this.ref = new AtomicReference<>(caps);
    }

    /** Returns the retained response and clears it (consume-once); {@code null} if empty or already taken. */
    @Nullable
    public FieldCapabilitiesResponse take() {
        return ref.getAndSet(null);
    }

    public boolean isEmpty() {
        return ref.get() == null;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PreResolvedFieldCaps;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "PreResolvedFieldCaps[" + (isEmpty() ? "empty" : "present") + "]";
    }
}
