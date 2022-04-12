/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

module org.elasticsearch.painless {
    requires org.elasticsearch.base;
    requires org.elasticsearch.geo;
    requires org.elasticsearch.painless.spi;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;

    requires antlr4.runtime;
    requires org.apache.lucene.core;
    requires org.objectweb.asm;
    requires org.objectweb.asm.commons;
    requires org.objectweb.asm.util;

    exports org.elasticsearch.painless;
    exports org.elasticsearch.painless.api;

    // java.lang.IllegalAccessError:
    // superclass access check failed: class org.elasticsearch.painless.PainlessScript$Script (in unnamed module @0x4a108434)
    // cannot access class org.elasticsearch.painless.action.PainlessExecuteAction$PainlessTestScript (in module org.elasticsearch.painless)
    // because module org.elasticsearch.painless does not export org.elasticsearch.painless.action to unnamed module @0x4a108434
    // at java.base/java.lang.ClassLoader.defineClass1(Native Method)
    // at java.base/java.lang.ClassLoader.defineClass(ClassLoader.java:1012)
    // at java.base/java.security.SecureClassLoader.defineClass(SecureClassLoader.java:150)
    // at org.elasticsearch.painless@8.2.0-SNAPSHOT/org.elasticsearch.painless.Compiler$Loader.defineScript(Compiler.java:115)
    // at org.elasticsearch.painless@8.2.0-SNAPSHOT/org.elasticsearch.painless.Compiler.compile(Compiler.java:225)
    // at org.elasticsearch.painless@8.2.0-SNAPSHOT/org.elasticsearch.painless.PainlessScriptEngine$2.run(PainlessScriptEngine.java:401)
    // at org.elasticsearch.painless@8.2.0-SNAPSHOT/org.elasticsearch.painless.PainlessScriptEngine$2.run(PainlessScriptEngine.java:397)
    // ...
    exports org.elasticsearch.painless.action;

    opens org.elasticsearch.painless to org.elasticsearch.painless.spi;  // whitelist access
    opens org.elasticsearch.painless.action to org.elasticsearch.server; // guice
}
