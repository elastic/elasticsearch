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

/**
 * Base class for tests of {@link Accountable#ramBytesUsed()} implementations that estimate an object's heap footprint field-by-field.
 * <p>
 * The core check is a structural tripwire: every non-static, non-primitive field declared on the class under test must be classified as
 * either <em>accounted for</em> (its heap cost is added explicitly in {@code ramBytesUsed()}) or <em>excluded</em> (deliberately not
 * counted, e.g. a derived cache or a documented gap). Primitive fields are ignored automatically because they are already covered by the
 * shallow instance size that every {@code ramBytesUsed()} implementation is expected to include. When someone adds or removes a reference
 * field without updating the estimate, this test fails, forcing them to decide how the new field should be accounted for.
 * <p>
 * This test is intentionally <em>not</em> a value assertion: it does not recompute the expected {@code ramBytesUsed()} using the same
 * formula the production code uses (which would be a tautology). Subclasses should add their own behavioural assertions (e.g. that
 * populating an optional field increases the reported size, or a hand-computed expected size for a fixed small instance) to catch
 * systematic over/under-counting that the structural check cannot.
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
     * Convenience for subclasses: the shallow size of the given instance, i.e. the lower bound that any correct {@code ramBytesUsed()}
     * must not go below.
     */
    protected static long shallowSizeOf(Accountable instance) {
        return RamUsageEstimator.shallowSizeOf(instance);
    }
}
