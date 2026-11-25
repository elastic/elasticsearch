/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Exposes a {@link StructSpecification @StructSpecification} interface's total byte size as a
 * method, without requiring the interface to extend {@link Addressable}. The annotated method must
 * return {@code int} and take no parameters.
 *
 * <p>The method contributes no field and does not affect the struct's layout. The generated
 * implementation returns the struct's resolved total byte size: a compile-time constant when the
 * layout is identical across every supported platform, or the running platform's resolved size
 * when the struct is a sparse per-platform layout.
 *
 * <pre>{@code
 * @StructSpecification
 * interface SockAddr {
 *     @Sizeof int sizeof();
 *     void sa_family(short v);
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Sizeof {
}
