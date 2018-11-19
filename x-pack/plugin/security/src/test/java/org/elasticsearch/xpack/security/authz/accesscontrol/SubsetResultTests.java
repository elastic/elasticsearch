/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import com.google.common.collect.Sets;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class SubsetResultTests extends ESTestCase {

    public void testConstructor() {
        final SubsetResult isASubsetResult = SubsetResult.isASubset();
        assertThat(isASubsetResult.result(), equalTo(SubsetResult.Result.YES));
        final SubsetResult isNotASubsetResult = SubsetResult.isNotASubset();
        assertThat(isNotASubsetResult.result(), equalTo(SubsetResult.Result.NO));
        final SubsetResult maybeASubsetResult = SubsetResult.mayBeASubset(Collections.singleton(Sets.newHashSet("abc", "def")));
        assertThat(maybeASubsetResult.result(), equalTo(SubsetResult.Result.MAYBE));
        assertThat(maybeASubsetResult.setOfIndexNamesForCombiningDLSQueries(),
                equalTo(Collections.singleton(Sets.newHashSet("abc", "def"))));
    }

    public void testEqualsHashCode() {
        final SubsetResult result = SubsetResult.mayBeASubset(Collections.singleton(Sets.newHashSet("abc", "def")));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(result, (original) -> {
            return SubsetResult.mayBeASubset(Collections.singleton(Sets.newHashSet("abc", "def")));
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(result, (original) -> {
            return SubsetResult.mayBeASubset(original.setOfIndexNamesForCombiningDLSQueries());
        }, SubsetResultTests::mutateTestItem);
    }

    private static SubsetResult mutateTestItem(SubsetResult original) {
        switch (randomIntBetween(0, 2)) {
        case 0:
            return SubsetResult.isASubset();
        case 1:
            return SubsetResult.isNotASubset();
        case 2:
            return SubsetResult.mayBeASubset(Collections.singleton(Sets.newHashSet(randomAlphaOfLength(5))));
        default:
            return SubsetResult.isNotASubset();
        }
    }
}
