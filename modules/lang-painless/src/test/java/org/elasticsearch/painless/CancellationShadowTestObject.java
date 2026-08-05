/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * Test fixture whose method name {@code each} deliberately collides with the
 * {@code @script_aware} {@code Iterable.each} augmentation. A {@code def}-typed call to
 * {@code each} is name-gated by the compiler (the name is in the cancellation-aware set) so the
 * synthetic script-this slot is pushed, but this method is an ordinary, non-cancellation-aware
 * method. It therefore exercises {@code Def.lookupMethod}'s fallback that discards the extra
 * script slot when the runtime-resolved method does not take a leading {@code PainlessScript}.
 */
public class CancellationShadowTestObject {

    public CancellationShadowTestObject() {}

    public int each(int value) {
        return value + 1;
    }
}
