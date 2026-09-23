/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/**
 * Verifies that in-scope {@link Accountable} implementations have a matching {@link AbstractAccountableFieldsTestCase} subclass.
 */
public final class AccountableFieldsTestCoverage {

    public static final Set<String> METADATA_LEAF_PACKAGES = Set.of(
        "org.elasticsearch.cluster.metadata",
        "org.elasticsearch.action.admin.indices.rollover",
        "org.elasticsearch.cluster.node",
        "org.elasticsearch.index.shard",
        "org.elasticsearch.common.compress"
    );

    private AccountableFieldsTestCoverage() {}

    public static void assertMetadataLeafCoverage() throws Exception {
        assertCoverage(METADATA_LEAF_PACKAGES);
    }

    public static void assertCoverage(Set<String> packages) throws Exception {
        Set<Class<?>> required = findRequiredAccountables(packages);
        Set<Class<?>> covered = findCoveredAccountables(packages);
        Set<Class<?>> missing = Sets.difference(required, covered);
        assertThat(
            "Accountable class(es) missing "
                + AbstractAccountableFieldsTestCase.class.getSimpleName()
                + ": "
                + missing.stream().map(Class::getName).sorted().collect(Collectors.joining(", ")),
            missing,
            empty()
        );
    }

    static Set<Class<?>> findRequiredAccountables(Set<String> packages) throws Exception {
        Set<Class<?>> required = new LinkedHashSet<>();
        for (String pkg : packages) {
            for (Class<?> clazz : loadClassesInPackage(pkg)) {
                if (requiresAccountableFieldsTest(clazz)) {
                    required.add(clazz);
                }
            }
        }
        return required;
    }

    static Set<Class<?>> findCoveredAccountables(Set<String> packages) throws Exception {
        Set<Class<?>> covered = new LinkedHashSet<>();
        for (String pkg : packages) {
            for (Class<?> clazz : loadClassesInPackage(pkg)) {
                if (isConcreteAccountableFieldsTestClass(clazz)) {
                    covered.add(readClassUnderTest(clazz));
                }
            }
        }
        return covered;
    }

    private static boolean requiresAccountableFieldsTest(Class<?> clazz) {
        if (clazz.isInterface() || clazz.isEnum() || clazz.isSynthetic()) {
            return false;
        }
        if (Accountable.class.isAssignableFrom(clazz) == false) {
            return false;
        }
        if (Modifier.isAbstract(clazz.getModifiers())) {
            // Sealed bases (e.g. IndexReshardingState) are covered by tests on each permitted subtype.
            return clazz.isSealed() == false;
        }
        // Concrete leaves of abstract non-sealed bases (e.g. MaxDocsCondition) are covered by a test on the base.
        return hasAbstractNonSealedAccountableSuperclass(clazz) == false;
    }

    private static boolean hasAbstractNonSealedAccountableSuperclass(Class<?> clazz) {
        for (Class<?> superClass = clazz.getSuperclass(); superClass != null && superClass != Object.class; superClass = superClass
            .getSuperclass()) {
            if (Accountable.class.isAssignableFrom(superClass)
                && Modifier.isAbstract(superClass.getModifiers())
                && superClass.isSealed() == false) {
                return true;
            }
        }
        return false;
    }

    private static boolean isConcreteAccountableFieldsTestClass(Class<?> clazz) {
        return AbstractAccountableFieldsTestCase.class.isAssignableFrom(clazz)
            && Modifier.isAbstract(clazz.getModifiers()) == false
            && clazz != AbstractAccountableFieldsTestCase.class;
    }

    @SuppressForbidden(reason = "test-only reflection to read classUnderTest() from convention tests")
    @SuppressWarnings("unchecked")
    private static Class<? extends Accountable> readClassUnderTest(Class<?> testClass) throws ReflectiveOperationException {
        AbstractAccountableFieldsTestCase test = (AbstractAccountableFieldsTestCase) testClass.getDeclaredConstructor().newInstance();
        Method method = AbstractAccountableFieldsTestCase.class.getDeclaredMethod("classUnderTest");
        method.setAccessible(true);
        return (Class<? extends Accountable>) method.invoke(test);
    }

    @SuppressForbidden(reason = "test-only classpath scan for accountable coverage conventions")
    private static List<Class<?>> loadClassesInPackage(String packageName) throws Exception {
        Set<String> classNames = new LinkedHashSet<>();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = loader.getResources(path);
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            if ("file".equals(resource.getProtocol()) == false) {
                continue;
            }
            Path directory = PathUtils.get(resource.toURI());
            if (Files.isDirectory(directory)) {
                collectClassNames(directory, packageName, classNames);
            }
        }
        List<Class<?>> classes = new ArrayList<>(classNames.size());
        for (String className : classNames.stream().sorted(Comparator.naturalOrder()).toList()) {
            classes.add(Class.forName(className, false, loader));
        }
        return classes;
    }

    private static void collectClassNames(Path directory, String pkg, Set<String> classNames) throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    collectClassNames(entry, pkg + "." + entry.getFileName(), classNames);
                } else if (entry.getFileName().toString().endsWith(".class")) {
                    String simpleName = entry.getFileName().toString().replace(".class", "");
                    classNames.add(pkg + "." + simpleName);
                }
            }
        }
    }
}
