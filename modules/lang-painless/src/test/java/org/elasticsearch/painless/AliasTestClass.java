/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class AliasTestClass {
    public static AliasedTestInnerClass getInnerAliased() {
        return new AliasedTestInnerClass();
    }

    public static UnaliasedTestInnerClass getInnerUnaliased() {
        return new UnaliasedTestInnerClass();
    }

    public static class AliasedTestInnerClass {
        public int plus(int a, int b) {
            return a + b;
        }
    }

    public static class UnaliasedTestInnerClass {
        public int minus(int a, int b) {
            return a - b;
        }
    }
}
