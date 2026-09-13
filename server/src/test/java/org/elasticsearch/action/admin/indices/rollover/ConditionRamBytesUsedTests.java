/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;
import org.elasticsearch.test.ClasspathUtils;

import java.lang.reflect.Modifier;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ConditionRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return Condition.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("name", "value");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        // Shared enum singleton; only the field reference is counted in SHALLOW_SIZE.
        return Set.of("type");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // RamUsageTester includes the Type enum instance; ramBytesUsed() does not, by design.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        return switch (randomIntBetween(0, 10)) {
            case 0 -> new MaxDocsCondition(randomNonNegativeLong());
            case 1 -> new MinDocsCondition(randomNonNegativeLong());
            case 2 -> new MaxPrimaryShardDocsCondition(randomNonNegativeLong());
            case 3 -> new MinPrimaryShardDocsCondition(randomNonNegativeLong());
            case 4 -> new MaxSizeCondition(randomByteSizeValue());
            case 5 -> new MinSizeCondition(randomByteSizeValue());
            case 6 -> new MaxPrimaryShardSizeCondition(randomByteSizeValue());
            case 7 -> new MinPrimaryShardSizeCondition(randomByteSizeValue());
            case 8 -> new MaxAgeCondition(randomTimeValue());
            case 9 -> new MinAgeCondition(randomTimeValue());
            case 10 -> new OptimalShardCountCondition(randomIntBetween(1, 100));
            default -> throw new AssertionError("unexpected condition branch");
        };
    }

    /**
     * Non-tautology check: the condition value (Long, ByteSizeValue, TimeValue, or Integer) must contribute a cost on top of the shallow
     * instance size.
     */
    public void testRamBytesUsedCountsConditionValue() {
        for (int i = 0; i < 10; i++) {
            Condition<?> instance = (Condition<?>) createRandomTestInstance();
            assertThat(instance.ramBytesUsed(), greaterThan(shallowSizeOf(instance)));
        }
    }

    /**
     * {@link Condition#ramBytesUsed()} uses {@code Condition}'s shallow size. A subclass that adds instance fields would be under-counted.
     */
    @SuppressForbidden(reason = "need access to all fields, including private instance fields")
    public void testConditionSubclassesDeclareNoFields() throws Exception {
        Set<Class<?>> leaves = discoverConcreteConditionSubclasses();

        Set<String> registeredNames = IndicesModule.getNamedWriteables()
            .stream()
            .filter(entry -> entry.categoryClass == Condition.class)
            .map(entry -> entry.name)
            .collect(Collectors.toUnmodifiableSet());
        Set<String> discoveredNames = leaves.stream()
            .map(ConditionRamBytesUsedTests::writeableName)
            .collect(Collectors.toUnmodifiableSet());
        assertThat(discoveredNames, equalTo(registeredNames));

        for (Class<?> leaf : leaves) {
            assertThat(
                leaf.getSimpleName() + " must not declare instance fields; Condition.ramBytesUsed() uses Condition's shallow size",
                Arrays.stream(leaf.getDeclaredFields()).filter(field -> Modifier.isStatic(field.getModifiers()) == false).toList(),
                empty()
            );
        }
    }

    private static Set<Class<?>> discoverConcreteConditionSubclasses() throws Exception {
        String pkg = Condition.class.getPackageName();
        Path[] roots = ClasspathUtils.findFilePaths(ConditionRamBytesUsedTests.class.getClassLoader(), pkg.replace('.', '/'));
        Set<Class<?>> leaves = new HashSet<>();
        for (Path root : roots) {
            if (Files.isDirectory(root) == false) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(root, "*.class")) {
                for (Path entry : stream) {
                    Class<?> clazz = Class.forName(pkg + "." + entry.getFileName().toString().replace(".class", ""));
                    if (clazz != Condition.class
                        && Condition.class.isAssignableFrom(clazz)
                        && Modifier.isAbstract(clazz.getModifiers()) == false) {
                        leaves.add(clazz);
                    }
                }
            }
        }
        return leaves;
    }

    private static String writeableName(Class<?> clazz) {
        try {
            return (String) clazz.getField("NAME").get(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(clazz.getName() + " must declare a public static NAME field", e);
        }
    }
}
