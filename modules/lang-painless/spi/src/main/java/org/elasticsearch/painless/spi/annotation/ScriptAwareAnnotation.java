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
 * Marks a whitelisted augmentation method as script-aware: in script contexts that activate it,
 * the lookup builder resolves the call to an augmentation overload that takes a leading
 * {@code PainlessScript} parameter, and the compiler emits {@code aload 0} (the script receiver)
 * ahead of the user-supplied arguments at every call site.  The augmentation body can then use the
 * script instance directly — for example {@code Iterable.each} uses it to poll the script's
 * cancellation runnable from inside its own iteration loop so long loops in non-Painless code
 * honour task cancellation.
 * <p>
 * In contexts that do not activate it, the annotation is silently dropped during lookup
 * resolution: the call resolves to whatever the whitelist line would have resolved to without the
 * annotation (an existing augmentation overload, or the direct JDK method).  Non-activated
 * contexts pay zero overhead.
 */
public class ScriptAwareAnnotation {

    public static final String NAME = "script_aware";

    public static final ScriptAwareAnnotation INSTANCE = new ScriptAwareAnnotation();

    private ScriptAwareAnnotation() {}
}
