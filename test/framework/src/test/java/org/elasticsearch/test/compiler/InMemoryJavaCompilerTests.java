/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.compiler;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.test.ESTestCase;

import java.lang.module.ModuleDescriptor;
import java.util.Map;

import static org.elasticsearch.test.compiler.InMemoryJavaCompiler.compile;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class InMemoryJavaCompilerTests extends ESTestCase {

    // mostly good enough if compile succeeds 1) without throwing, and 2) returns non-null

    public void testCompileBasic() {
        assertThat(compile("Foo", "public class Foo { }"), notNullValue());
    }

    public void testCompilePackage() {
        assertThat(compile("p.Foo", "package p; public class Foo { }"), notNullValue());
    }

    public void testCompileSerializableRecord() {
        assertThat(compile("Foo", "public record Foo () implements java.io.Serializable { }"), notNullValue());
    }

    public void testCompileModule() throws Exception {
        byte[] ba = compile("module-info", "module foo { requires java.base; }");
        assertThat(ba, notNullValue());
        var md = ModuleDescriptor.read(new ByteArrayStreamInput(ba));
        assertThat(md.name(), equalTo("foo"));
    }

    public void testCompileModuleWithExports() {
        Map<String, CharSequence> sources = Map.of("module-info", """
            module foo {
              exports p;
            }
            """, "p.Foo", """
            package p;
            public class Foo implements java.util.function.Supplier<String> {
              @Override public String get() {
                return "Hello World!";
              }
            }
            """);
        var result = compile(sources);
        assertThat(result, notNullValue());
        assertThat(result, allOf(hasEntry(is("module-info"), notNullValue()), hasEntry(is("p.Foo"), notNullValue())));
    }

    public void testCompileModuleProvider() {
        Map<String, CharSequence> sources = Map.of("module-info", """
            module x.foo.impl {
              exports p;
              opens q;
              provides java.util.function.IntSupplier with p.FooIntSupplier;
            }
            """, "p.FooIntSupplier", """
            package p;
            public class FooIntSupplier implements java.util.function.IntSupplier, q.MsgSupplier {
              @Override public int getAsInt() {
                return 12;
              }
              @Override public String msg() {
                return "Hello from FooIntSupplier";
              }
            }
            """, "q.MsgSupplier", """
            package q;
            public interface MsgSupplier {
              String msg();
            }
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);
        var result = compile(sources);
        assertThat(result, notNullValue());
        assertThat(
            result,
            allOf(
                hasEntry(is("module-info"), notNullValue()),
                hasEntry(is("p.FooIntSupplier"), notNullValue()),
                hasEntry(is("q.MsgSupplier"), notNullValue())
            )
        );
    }

    static final Class<RuntimeException> RE = RuntimeException.class;

    public void testCompileFailure() {
        // Expect:
        // /p/Foo.java:1: error: cannot find symbol
        // package p; public class Foo extends Bar { }
        // ^
        // symbol: class Bar
        // 1 error
        var e = expectThrows(RE, () -> compile("p.Foo", "package p; public class Foo extends Bar { }"));
        assertThat(e.getMessage(), containsString("Could not compile p.Foo with source code"));
    }
}
