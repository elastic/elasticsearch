/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.hamcrest;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleDescriptor.Exports;
import java.lang.module.ModuleDescriptor.Opens;
import java.lang.module.ModuleDescriptor.Provides;
import java.lang.module.ModuleDescriptor.Requires;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.exportsOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.opensOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.providesOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.requiresOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ModuleDescriptorMatchersTests extends ESTestCase {

    // -- Exports

    public void testExportsPackage() {
        var exports = createExports("p");
        assertMatch(exports, exportsOf("p"));
    }

    public void testExportsPackage2() {
        var exports = createExports("foo.bar");
        assertMatch(exports, exportsOf("foo.bar"));
    }

    public void testExportsQualifiedExport() {
        var exports = createExports("p.q", Set.of("mod", "mod1", "mod2"));
        assertMatch(exports, exportsOf("p.q", Set.of("mod", "mod1", "mod2")));
    }

    public void testExportsQualifiedExportMismatch() {
        var exports = createExports("p", Set.of("m"));
        assertMismatch(exports, exportsOf("p.foo", Set.of("mod")), equalTo("Exports[p.foo to [mod]], actual Exports[p to [m]]"));
    }

    public void testExportsQualifiedExportMismatch2() {
        var exports = createExports("p", Set.of("m"));
        assertMismatch(exports, exportsOf("p", Set.of("mod")), equalTo("Exports[p to [mod]], actual Exports[p to [m]]"));
    }

    public void testExportsNull() {
        assertMismatch(null, exportsOf("p"), equalTo("was null"));
    }

    static Exports createExports(String pkg) {
        var md = ModuleDescriptor.newModule("m").exports(pkg).build();
        return md.exports().stream().findFirst().orElseThrow();
    }

    static Exports createExports(String pkg, Set<String> targets) {
        var builder = ModuleDescriptor.newModule("m");
        builder.exports(pkg, Set.copyOf(targets));
        return builder.build().exports().stream().findFirst().orElseThrow();
    }

    // -- Opens

    public void testOpensPackage() {
        var opens = createOpens("p");
        assertMatch(opens, opensOf("p"));
    }

    public void testOpensPackage2() {
        var opens = createOpens("larry.curly.moe");
        assertMatch(opens, opensOf("larry.curly.moe"));
    }

    public void testOpensQualifiedOpens() {
        var opens = createOpens("p.q", Set.of("mod", "mod1", "mod2"));
        assertMatch(opens, opensOf("p.q", Set.of("mod", "mod1", "mod2")));
    }

    public void testOpensQualifiedExportMismatch() {
        var opens = createOpens("p", Set.of("m"));
        assertMismatch(opens, opensOf("p.foo", Set.of("mod")), equalTo("Opens[p.foo to [mod]], actual Opens[p to [m]]"));
    }

    public void testOpensQualifiedExportMismatch2() {
        var opens = createOpens("p", Set.of("m"));
        assertMismatch(opens, opensOf("p", Set.of("mod")), equalTo("Opens[p to [mod]], actual Opens[p to [m]]"));
    }

    public void testOpensNull() {
        assertMismatch(null, opensOf("p"), equalTo("was null"));
    }

    static Opens createOpens(String pkg) {
        var md = ModuleDescriptor.newModule("m").opens(pkg).build();
        return md.opens().stream().findFirst().orElseThrow();
    }

    static Opens createOpens(String pkg, Set<String> targets) {
        var builder = ModuleDescriptor.newModule("m");
        builder.opens(pkg, Set.copyOf(targets));
        return builder.build().opens().stream().findFirst().orElseThrow();
    }

    // -- Requires

    public void testRequiresModule() {
        var requires = createRequires("m");
        assertMatch(requires, requiresOf("m"));
    }

    public void testRequiresModule1() {
        var requires = createRequires("m1");
        assertMatch(requires, requiresOf("m1"));
    }

    public void testRequiresMismatch() {
        var requires = createRequires("m");
        assertMismatch(requires, requiresOf("foo"), equalTo("Requires with name foo, actual Requires with name m"));
    }

    public void testRequiresMismatch2() {
        var requires = createRequires("o.e.server");
        assertMismatch(requires, requiresOf("bar.baz"), equalTo("Requires with name bar.baz, actual Requires with name o.e.server"));
    }

    public void testRequiresNull() {
        assertMismatch(null, requiresOf("m"), equalTo("was null"));
    }

    static Requires createRequires(String mn) {
        var md = ModuleDescriptor.newModule("mmm").requires(mn).build();
        return md.requires().stream().filter(r -> r.name().equals(mn)).findFirst().orElseThrow();
    }

    // --- provides

    public void testProvides() {
        var provides = createProvides("p.FooService", List.of("q.FooServiceImpl"));
        assertMatch(provides, providesOf("p.FooService", List.of("q.FooServiceImpl")));
    }

    public void testProvidesMismatch() {
        var provides = createProvides("p.F", List.of("q.FI"));
        assertMismatch(
            provides,
            providesOf("p.F", List.of("q.BI")),
            equalTo("Provides[p.F with [q.BI]], actual Provides[p.F with [q.FI]]")
        );
    }

    public void testProvidesMismatch2() {
        var provides = createProvides("p.F", List.of("q.FI"));
        assertMismatch(
            provides,
            providesOf("p.B", List.of("q.FI")),
            equalTo("Provides[p.B with [q.FI]], actual Provides[p.F with [q.FI]]")
        );
    }

    public void testProvidesMismatch3() {
        var provides = createProvides("p.F", List.of("q.FI"));
        assertMismatch(
            provides,
            providesOf("p.F", List.of("q.FI", "q.BI")),
            equalTo("Provides[p.F with [q.FI, q.BI]], actual Provides[p.F with [q.FI]]")
        );
    }

    public void testProvidesNull() {
        assertMismatch(null, providesOf("p.Foo", List.of("p.FooImpl")), equalTo("was null"));
    }

    static Provides createProvides(String service, List<String> providers) {
        var md = ModuleDescriptor.newModule("m").provides(service, List.copyOf(providers)).build();
        return md.provides().stream().findFirst().orElseThrow();
    }

    // -- infra

    static <T> void assertMatch(T actual, Matcher<? super T> matcher) {
        assertMatch("", actual, matcher);
    }

    static <T> void assertMatch(String reason, T actual, Matcher<? super T> matcher) {
        if (matcher.matches(actual)) {
            return;
        }
        Description description = new StringDescription();
        description.appendText("expected ");
        matcher.describeMismatch(actual, description);
        throw new AssertionError(description.toString());
    }

    static <T> void assertMismatch(T actual, Matcher<? super T> matcher, Matcher<String> mismatchDescriptionMatcher) {
        assertThat(matcher.matches(actual), is(false));

        StringDescription description = new StringDescription();
        matcher.describeMismatch(actual, description);
        assertThat(description.toString(), mismatchDescriptionMatcher);
    }
}
