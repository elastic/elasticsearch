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
import java.util.Objects;
import java.util.Set;

public class ModuleDescriptorMatchers {

    private ModuleDescriptorMatchers() {}

    public static Matcher<Exports> exportsOf(String pkg) {
        return new ExportsMatcher(pkg, Set.of());
    }

    public static Matcher<Exports> exportsOf(String pkg, Set<String> targets) {
        return new ExportsMatcher(pkg, targets);
    }

    public static Matcher<Opens> opensOf(String pkg) {
        return new OpensMatcher(pkg, Set.of());
    }

    public static Matcher<Opens> opensOf(String pkg, Set<String> targets) {
        return new OpensMatcher(pkg, targets);
    }

    public static Matcher<Requires> requiresOf(String mn) {
        return new RequiresNameMatcher(mn);
    }

    public static Matcher<Provides> providesOf(String service, List<String> provides) {
        return new ProvidesMatcher(service, provides);
    }

    /**
     * Matcher that matches the <i>source</i> and <i>targets</i> of a {@code Requires}.
     * The matcher is agnostic of the {@code Requires} modifiers.
     */
    static class ExportsMatcher extends TypeSafeMatcher<Exports> {

        private final String source;
        private final Set<String> targets;

        ExportsMatcher(String source, Set<String> targets) {
            this.source = source;
            this.targets = Set.copyOf(targets);
        }

        @Override
        protected boolean matchesSafely(final Exports item) {
            return item != null && Objects.equals(item.source(), source) && Objects.equals(item.targets(), targets);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(String.format(java.util.Locale.ROOT, "Exports[%s]", exportsToString(source, targets)));
        }

        @Override
        protected void describeMismatchSafely(final Exports item, final Description mismatchDescription) {
            describeTo(mismatchDescription);
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(String.format(java.util.Locale.ROOT, ", actual Exports[%s]", exportsToString(item)));
            }
        }

        private static String exportsToString(String source, Set<String> targets) {
            if (targets.isEmpty()) {
                return source;
            } else {
                return source + " to " + targets;
            }
        }

        private static String exportsToString(Exports exports) {
            if (exports.targets().isEmpty()) {
                return exports.source();
            } else {
                return exports.source() + " to " + exports.targets();
            }
        }
    }

    /**
     * Matcher that matches the <i>name</i> of a {@code Requires}.
     * The matcher is agnostic of other {@code Requires} state, like the modifiers.
     */
    static class RequiresNameMatcher extends TypeSafeMatcher<Requires> {

        private final String mn;

        RequiresNameMatcher(String mn) {
            this.mn = mn;
        }

        @Override
        protected boolean matchesSafely(final Requires item) {
            return item != null && Objects.equals(item.name(), mn);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Requires with name " + mn);
        }

        @Override
        protected void describeMismatchSafely(final Requires item, final Description mismatchDescription) {
            describeTo(mismatchDescription);
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(", actual Requires with name " + item);
            }
        }
    }

    /**
     * Matcher that matches the <i>source</i> and <i>targets</i> of an {@code Opens}.
     * The matcher is agnostic of the modifiers.
     */
    static class OpensMatcher extends TypeSafeMatcher<Opens> {

        private final String source;
        private final Set<String> targets;

        OpensMatcher(String source, Set<String> targets) {
            this.source = source;
            this.targets = Set.copyOf(targets);
        }

        @Override
        protected boolean matchesSafely(final Opens item) {
            return item != null && Objects.equals(item.source(), source) && Objects.equals(item.targets(), targets);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(String.format(java.util.Locale.ROOT, "Opens[%s]", opensToString(source, targets)));
        }

        @Override
        protected void describeMismatchSafely(final Opens item, final Description mismatchDescription) {
            describeTo(mismatchDescription);
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(String.format(java.util.Locale.ROOT, ", actual Opens[%s]", opensToString(item)));
            }
        }

        private static String opensToString(String source, Set<String> targets) {
            if (targets.isEmpty()) {
                return source;
            } else {
                return source + " to " + targets;
            }
        }

        private static String opensToString(Opens opens) {
            if (opens.targets().isEmpty()) {
                return opens.source();
            } else {
                return opens.source() + " to " + opens.targets();
            }
        }
    }

    /**
     * Matcher that matches the <i>service</i> and <i>providers</i> of a {@code Provides}.
     */
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
            description.appendText(String.format(java.util.Locale.ROOT, "Provides[%s]", providesToString(service, providers)));
        }

        @Override
        protected void describeMismatchSafely(final Provides item, final Description mismatchDescription) {
            describeTo(mismatchDescription);
            if (item == null) {
                mismatchDescription.appendText("was null");
            } else {
                mismatchDescription.appendText(String.format(java.util.Locale.ROOT, ", actual Provides[%s]", item));
            }
        }

        private static String providesToString(String service, List<String> provides) {
            return service + " with " + provides;
        }
    }
}
