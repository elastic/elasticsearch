/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.spi.annotation;

/**
 * Marks a whitelisted method as cancellation-aware: in script contexts whose base class
 * supports cancellation (overrides {@code _getCancellationCheck()}), the lookup builder
 * resolves the call to an augmentation overload that takes a leading {@code PainlessScript}
 * parameter, and the compiler emits {@code aload 0} (the script receiver) ahead of the
 * user-supplied arguments at every call site.  The augmentation body uses the receiver to
 * fetch the cancel {@code Runnable} via {@code _getCancellationCheck()} and poll it from
 * inside its iteration loop so that long-running loops in non-Painless code (e.g. the
 * driver loop of {@code Iterable.forEach} or the augmentation's own for-loop) honour task
 * cancellation.
 * <p>
 * In script contexts that do not support cancellation, the annotation is silently dropped
 * during lookup resolution: the call resolves to whatever the whitelist line would have
 * resolved to without the annotation (an existing augmentation overload, or the direct JDK
 * method).  Non-cancellation-aware contexts pay zero overhead.
 */
public class CancellationAwareAnnotation {

    public static final String NAME = "cancellation_aware";

    public static final CancellationAwareAnnotation INSTANCE = new CancellationAwareAnnotation();

    private CancellationAwareAnnotation() {}
}
