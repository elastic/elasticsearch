/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector.step;

/**
 * Performs the paperwork associated with inheritance.
 * Causes all objects already associated with <code>subtype</code> also to be
 * associated with <code>requestedType</code>.
 * Subsequent steps that request the latter will find the former.
 * Lists of the latter will include instances of the former.
 *
 * <p>
 * Note that this is not a permanent "link" between the types.
 * It is more like a copy operation, but we avoid the name <code>CopyStep</code>
 * because that might imply that the objects are being copied,
 * while actually it's only metadata getting copied.
 */
public record RollUpStep(
   Class<?> requestedType,
   Class<?> subtype
) implements InstanceSupplyingStep { }
