/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Base class for tests of {@link Accountable#ramBytesUsed()} implementations that estimate an object's heap footprint field-by-field.
 * <p>
 * The core check is a structural tripwire: every non-static, non-primitive field declared on the class under test must be classified as
 * either <em>accounted for</em> (its heap cost is added explicitly in {@code ramBytesUsed()}) or <em>excluded</em> (deliberately not
 * counted, e.g. a derived cache or a documented gap). Primitive fields are ignored automatically because they are already covered by the
 * shallow instance size that every {@code ramBytesUsed()} implementation is expected to include. When someone adds or removes a reference
 * field without updating the estimate, this test fails, forcing them to decide how the new field should be accounted for.
 * <p>
 * When {@link #assertsAgainstRamUsageTester()} is true, a value check also asserts that {@code ramBytesUsed()} never under-counts the
 * retained heap measured by Lucene's {@link RamUsageTester}. Subclasses that deliberately omit shared/interned state (e.g. interned
 * {@code Settings} strings, shared enum singletons, or cross-index deduplicated mappings) should override that hook to {@code false} and
 * document why a full-graph comparison would fail by design; those classes still get the structural tripwire and should keep their own
 * behavioural assertions.
 */
public abstract class AbstractAccountableFieldsTestCase extends ESTestCase {

    /**
     * @return the concrete {@link Accountable} class whose fields are checked. Only fields declared directly on this class are inspected;
     * fields inherited from a superclass are the superclass's own responsibility to account for and test.
     */
    protected abstract Class<? extends Accountable> classUnderTest();

    /**
     * @return names of reference fields whose heap cost is added explicitly in {@code ramBytesUsed()}.
     */
    protected abstract Set<String> fieldsAccountedForInRamBytesUsed();

    /**
     * @return names of reference fields deliberately not counted in {@code ramBytesUsed()}, each of which should have a comment (in the
     * subclass or the production code) explaining why it needs no accounting.
     */
    protected abstract Set<String> fieldsExcludedFromRamBytesUsed();

    /**
     * A populated instance of {@link #classUnderTest()} for {@link #testRamBytesUsedNeverUnderCountsActualHeap()}. Subclasses that opt out
     * via {@link #assertsAgainstRamUsageTester()} need not override this.
     */
    protected Accountable createTestInstance() {
        throw new UnsupportedOperationException(
            classUnderTest().getSimpleName() + " must implement createTestInstance() when assertsAgainstRamUsageTester() is true"
        );
    }

    /**
     * Whether to assert {@code ramBytesUsed() >= RamUsageTester.ramUsed(...)}. Defaults to {@code true}. Override to {@code false} when the
     * estimate deliberately under-counts shared or interned state that a full object-graph walk would include.
     */
    protected boolean assertsAgainstRamUsageTester() {
        return true;
    }

    @SuppressForbidden(reason = "need the names of all declared fields, most of which are private")
    public void testRamBytesUsedAccountsForAllReferenceFields() {
        final Class<? extends Accountable> clazz = classUnderTest();
        final Set<String> accounted = fieldsAccountedForInRamBytesUsed();
        final Set<String> excluded = fieldsExcludedFromRamBytesUsed();

        assertThat(
            "a field cannot be both accounted for and excluded in [" + clazz.getSimpleName() + "]",
            Sets.intersection(accounted, excluded),
            empty()
        );

        // Primitive fields are already covered by RamUsageEstimator.shallowSizeOf(...); only reference fields must be classified.
        final Set<String> referenceFields = Arrays.stream(clazz.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()) == false)
            .filter(f -> f.getType().isPrimitive() == false)
            .map(Field::getName)
            .collect(Collectors.toSet());

        assertThat(
            "reference field(s) on ["
                + clazz.getSimpleName()
                + "] are not classified for heap accounting - add each new field to the accounted-for set (and add its cost to "
                + "ramBytesUsed()) or to the excluded set (with a comment explaining why it needs no accounting)",
            referenceFields,
            equalTo(Sets.union(accounted, excluded))
        );
    }

    /**
     * Value check against a real object-graph measurement. Catches systematic under-counting that the structural tripwire cannot.
     */
    public void testRamBytesUsedNeverUnderCountsActualHeap() {
        assumeTrue(
            classUnderTest().getSimpleName() + " deliberately under-counts shared/interned state vs a full-graph RamUsageTester walk",
            assertsAgainstRamUsageTester()
        );
        Accountable instance = createTestInstance();
        long estimate = instance.ramBytesUsed();
        long actual = RamUsageTester.ramUsed(instance);
        assertThat(
            "estimate under-counts retained heap: estimate=" + estimate + " actual=" + actual + " for " + classUnderTest().getSimpleName(),
            estimate,
            greaterThanOrEqualTo(actual)
        );
    }

    /**
     * Convenience for subclasses: the shallow size of the given instance, i.e. the lower bound that any correct {@code ramBytesUsed()}
     * must not go below.
     */
    protected static long shallowSizeOf(Accountable instance) {
        return RamUsageEstimator.shallowSizeOf(instance);
    }
}
