/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

/**
 * Creates an alias in PainlessLookupBuilder for the given class.  Can be used to expose an inner class without
 * scripts and whitelists needing to scope it by the outer class.
 *
 * For class
 *
 * <pre>
 * public class Outer {
 *     public static class Inner {
 *
 *     }
 *     public Inner inner() {
 *         return new Inner();
 *     }
 * }
 * </pre>
 *
 * Normally scripts would need to reference {@code Outer.Inner}.
 *
 * With an alias annotation {@code @alias[class="Inner"]} on the class
 * <pre>
 * class Outer$Inner @alias[class="AliasedTestInnerClass"] {
 * }
 * </pre>
 *
 * Then whitelist can have {@code Inner} as the return value for {@code inner} instead of {@code Outer.Inner}
 * <pre>
 * class Outer {
 *   Inner inner()
 * }
 * </pre>
 *
 * And scripts refer can to {@code Inner} directly, {@code Inner inner = Outer.inner()}, instead of using the outer class to scope
 * the type name {@code Outer.Inner} as would normally be required {@code Outer.Inner inner = Outer.inner()}
 *
 * Only class alias types are available.
 *
 * @param alias the other name for the class
 */
public record AliasAnnotation(String alias) {
    public static final String NAME = "alias";
}
