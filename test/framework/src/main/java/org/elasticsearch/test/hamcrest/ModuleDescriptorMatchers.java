/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.hamcrest;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.lang.module.ModuleDescriptor.Exports;
import java.lang.module.ModuleDescriptor.Opens;
import java.lang.module.ModuleDescriptor.Provides;
import java.lang.module.ModuleDescriptor.Requires;
import java.util.List;
import java.util.Set;

public class ModuleDescriptorMatchers {

    private ModuleDescriptorMatchers() {}

    public static Matcher<Exports> exportsOf(String pkg) {
        return new ExportsMatcher(pkg, Set.of(), Set.of());
    }

    public static Matcher<Exports> exportsOf(String pkg, Set<String> targets) {
        return new ExportsMatcher(pkg, targets, Set.of());
    }

    public static Matcher<Opens> opensOf(String mn) {
        return new OpensSourceMatcher(mn);
    }

    public static Matcher<Requires> requiresOf(String mn) {
        return new RequiresNameMatcher(mn);
    }

    public static Matcher<Provides> providesOf(String service, List<String> provides) {
        return new ProvidesMatcher(service, provides);
    }

    static class ExportsMatcher extends TypeSafeMatcher<Exports> {

        private final String source;
        private final Set<String> targets;
        private final Set<Exports.Modifier> modifiers;

        ExportsMatcher(String source, Set<String> targets, Set<Exports.Modifier> modifiers) {
            this.source = source;
            this.targets = Set.copyOf(targets);
            this.modifiers = Set.copyOf(modifiers);
        }

        @Override
        protected boolean matchesSafely(final Exports item) {
            return item != null && item.source().equals(source) && item.targets().equals(targets) && item.modifiers().equals(modifiers);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Export, source=%s, targets=%s, modifiers=%s".formatted(source, targets, modifiers));
        }

        @Override
        protected void describeMismatchSafely(final Exports item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(item.toString());
            }
        }
    }

    static class RequiresNameMatcher extends TypeSafeMatcher<Requires> {

        private final String mn;

        RequiresNameMatcher(String mn) {
            this.mn = mn;
        }

        @Override
        protected boolean matchesSafely(final Requires item) {
            return item != null && item.name().equals(mn);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Requires with name " + mn);
        }

        @Override
        protected void describeMismatchSafely(final Requires item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(item.name());
            }
        }
    }

    static class OpensSourceMatcher extends TypeSafeMatcher<Opens> {

        private final String pkg;

        OpensSourceMatcher(String pkg) {
            this.pkg = pkg;
        }

        @Override
        protected boolean matchesSafely(final Opens item) {
            return item != null && item.source().equals(pkg);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Opens with name " + pkg);
        }

        @Override
        protected void describeMismatchSafely(final Opens item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(item.source());
            }
        }
    }

    static class ProvidesMatcher extends TypeSafeMatcher<Provides> {

        private final String service;
        private final List<String> providers;

        ProvidesMatcher(String service, List<String> providers) {
            this.service = service;
            this.providers = List.copyOf(providers);
        }

        @Override
        protected boolean matchesSafely(final Provides item) {
            return item != null && item.service().equals(service) && item.providers().equals(providers);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Provides, service=%s, providers=%s".formatted(service, providers));
        }

        @Override
        protected void describeMismatchSafely(final Provides item, final Description mismatchDescription) {
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(item.toString());
            }
        }
    }
}
