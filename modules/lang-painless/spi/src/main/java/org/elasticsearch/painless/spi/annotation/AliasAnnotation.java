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
 * scripts and whitelists needing to scoping it by the outer class.
 *
 * For class
 * public class Outer {
 *     public static class Inner {
 *
 *     }
 *     public Inner inner() {
 *         return new Inner();
 *     }
 * }
 *
 * Normally scripts would need to reference Outer.Inner.
 *
 * With an alias annotation @alias[class="Inner"] on the class
 * class Outer$Inner @alias[class="AliasedTestInnerClass"] {
 * }
 *
 * The whitelist can have use the alias
 *
 * class Outer {
 *   Inner inner()
 * }
 *
 * And scripts can do "Inner inner = Outer.inner()" instead of "Outer.Inner inner = Outer.inner()"
 *
 * Only class alias types are available.
 *
 * @param alias the other way to refer to the class
 */
public record AliasAnnotation(String alias) {
    public static final String NAME = "alias";
}
