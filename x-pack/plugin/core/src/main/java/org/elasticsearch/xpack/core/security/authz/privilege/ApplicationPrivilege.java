/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.Strings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * An application privilege has an application name (e.g. {@code "my-app"}) that identifies an application (that exists
 * outside of elasticsearch), a privilege name (e.g. {@code "admin}) that is meaningful to that application, and zero or
 * more "action patterns" (e.g {@code "admin/user/*", "admin/team/*"}).
 * Action patterns must contain at least one special character from ({@code /}, {@code :}, {@code *}) to distinguish them
 * from privilege names.
 * The action patterns are entirely optional - many application will find that simple "privilege names" are sufficient, but
 * they allow applications to define high level abstract privileges that map to multiple low level capabilities.
 */
public final class ApplicationPrivilege extends Privilege {

    private static final Pattern VALID_APPLICATION = Pattern.compile("^[a-z][A-Za-z0-9_-]{2,}$");
    private static final Pattern VALID_APPLICATION_OR_WILDCARD = Pattern.compile("^[a-z*][A-Za-z0-9_*-]*");
    private static final Pattern VALID_NAME = Pattern.compile("^[a-z][a-zA-Z0-9_.-]*$");

    /**
     * A name or action must be composed of printable, visible ASCII characters.
     * That is: letters, numbers &amp; symbols, but no whitespace.
     */
    private static final Pattern VALID_NAME_OR_ACTION = Pattern.compile("^\\p{Graph}*$");

    public static final Function<String, ApplicationPrivilege> NONE = app -> new ApplicationPrivilege(app, "none", new String[0]);

    private final String application;
    private final String[] patterns;

    public ApplicationPrivilege(String application, String privilegeName, String... patterns) {
        this(application, Collections.singleton(privilegeName), patterns);
    }

    public ApplicationPrivilege(String application, Set<String> name, String... patterns) {
        super(name, patterns);
        this.application = application;
        this.patterns = patterns;
    }

    public String getApplication() {
        return application;
    }

    // Package level for testing
    String[] getPatterns() {
        return patterns;
    }

    /**
     * Validate that the provided application name is valid, and throws an exception otherwise
     *
     * @throws IllegalArgumentException if the name is not valid
     */
    public static void validateApplicationName(String application) {
        validateApplicationName(application, VALID_APPLICATION);
    }

    /**
     * Validate that the provided name is a valid application, or a wildcard pattern for an application and throws an exception otherwise
     *
     * @throws IllegalArgumentException if the name is not valid
     */
    public static void validateApplicationNameOrWildcard(String application) {
        validateApplicationName(application, VALID_APPLICATION_OR_WILDCARD);
    }

    private static void validateApplicationName(String application, Pattern pattern) {
        if (pattern.matcher(application).matches() == false) {
            throw new IllegalArgumentException("Application names must match the pattern " + pattern.pattern()
                + " (but was '" + application + "')");
        }
    }

    /**
     * Validate that the provided privilege name is valid, and throws an exception otherwise
     *
     * @throws IllegalArgumentException if the name is not valid
     */
    public static void validatePrivilegeName(String name) {
        if (isValidPrivilegeName(name) == false) {
            throw new IllegalArgumentException("Application privilege names must match the pattern " + VALID_NAME.pattern()
                + " (found '" + name + "')");
        }
    }

    private static boolean isValidPrivilegeName(String name) {
        return VALID_NAME.matcher(name).matches();
    }

    /**
     * Validate that the provided name is a valid privilege name or action name, and throws an exception otherwise
     *
     * @throws IllegalArgumentException if the name is not valid
     */
    public static void validatePrivilegeOrActionName(String name) {
        if (VALID_NAME_OR_ACTION.matcher(name).matches() == false) {
            throw new IllegalArgumentException("Application privilege names and actions must match the pattern "
                + VALID_NAME_OR_ACTION.pattern() + " (found '" + name + "')");
        }
    }

    /**
     * Finds or creates an application privileges with the provided names.
     * Each element in {@code name} may be the name of a stored privilege (to be resolved from {@code stored}, or a bespoke action pattern.
     */
    public static ApplicationPrivilege get(String application, Set<String> name, Collection<ApplicationPrivilegeDescriptor> stored) {
        if (name.isEmpty()) {
            return NONE.apply(application);
        } else {
            Map<String, ApplicationPrivilegeDescriptor> lookup = stored.stream()
                .filter(apd -> apd.getApplication().equals(application))
                .collect(Collectors.toMap(ApplicationPrivilegeDescriptor::getName, Function.identity()));
            return resolve(application, name, lookup);
        }
    }

    private static ApplicationPrivilege resolve(String application, Set<String> names, Map<String, ApplicationPrivilegeDescriptor> lookup) {
        final int size = names.size();
        if (size == 0) {
            throw new IllegalArgumentException("empty set should not be used");
        }

        Set<String> actions = new HashSet<>();
        Set<String> patterns = new HashSet<>();
        for (String name : names) {
            name = name.toLowerCase(Locale.ROOT);
            if (isValidPrivilegeName(name)) {
                ApplicationPrivilegeDescriptor descriptor = lookup.get(name);
                if (descriptor != null) {
                    patterns.addAll(descriptor.getActions());
                } else {
                    throw new IllegalArgumentException("unknown application privilege [" + name + "]");
                }
            } else {
                actions.add(name);
            }
        }

        patterns.addAll(actions);
        return new ApplicationPrivilege(application, names, patterns.toArray(new String[patterns.size()]));
    }

    @Override
    public String toString() {
        return application + ":" + super.toString() + "(" + Strings.arrayToCommaDelimitedString(patterns) + ")";
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(application);
        result = 31 * result + Arrays.hashCode(patterns);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o)
            && Objects.equals(this.application, ((ApplicationPrivilege) o).application)
            && Arrays.equals(this.patterns, ((ApplicationPrivilege) o).patterns);
    }

}
