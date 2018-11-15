/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import com.google.common.collect.Sets;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class SubsetResultTests extends ESTestCase {

    public void testConstructor() {
        final SubsetResult isASubsetResult = SubsetResult.isASubset();
        assertThat(isASubsetResult.result(), equalTo(SubsetResult.Result.YES));
        final SubsetResult isNotASubsetResult = SubsetResult.isNotASubset();
        assertThat(isNotASubsetResult.result(), equalTo(SubsetResult.Result.NO));
        final SubsetResult maybeASubsetResult = SubsetResult.mayBeASubset(Sets.newHashSet("abc", "def"));
        assertThat(maybeASubsetResult.result(), equalTo(SubsetResult.Result.MAYBE));
        assertThat(maybeASubsetResult.setOfIndexNamesForCombiningDLSQueries(),
                equalTo(Collections.singleton(Sets.newHashSet("abc", "def"))));
    }

    public void testMergeWhenOneIsNull() {
        final SubsetResult isASubsetResult = SubsetResult.isASubset();

        final SubsetResult result1 = SubsetResult.merge(null, isASubsetResult);
        final SubsetResult result2 = SubsetResult.merge(isASubsetResult, null);
        assertThat(result1.result(), equalTo(SubsetResult.Result.YES));
        assertThat(result1.result(), equalTo(result2.result()));
        assertThat(result1.setOfIndexNamesForCombiningDLSQueries(), equalTo(result2.setOfIndexNamesForCombiningDLSQueries()));
    }

    public void testMergeWhenBothAreYes() {
        final SubsetResult isASubsetResult1 = SubsetResult.isASubset();
        final SubsetResult isASubsetResult2 = SubsetResult.isASubset();
        final SubsetResult result = SubsetResult.merge(isASubsetResult1, isASubsetResult2);
        assertThat(result.result(), equalTo(SubsetResult.Result.YES));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(Collections.emptySet()));
    }

    public void testMergeWhenBothAreNo() {
        final SubsetResult isNotASubsetResult1 = SubsetResult.isNotASubset();
        final SubsetResult isNotASubsetResult2 = SubsetResult.isNotASubset();
        final SubsetResult result = SubsetResult.merge(isNotASubsetResult1, isNotASubsetResult2);
        assertThat(result.result(), equalTo(SubsetResult.Result.NO));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(Collections.emptySet()));
    }

    public void testMergeWhenOneIsYesAndOtherIsNo() {
        final SubsetResult isASubsetResult1 = SubsetResult.isASubset();
        final SubsetResult isNotASubsetResult2 = SubsetResult.isNotASubset();
        final SubsetResult result = SubsetResult.merge(isASubsetResult1, isNotASubsetResult2);
        assertThat(result.result(), equalTo(SubsetResult.Result.YES));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(Collections.emptySet()));
    }

    public void testMergeWhenOneIsYesAndOtherIsMaybe() {
        final SubsetResult isASubsetResult1 = SubsetResult.isASubset();
        final SubsetResult maybeASubsetResult2 = SubsetResult.mayBeASubset(Sets.newHashSet("abc", "def"));
        final SubsetResult result = SubsetResult.merge(isASubsetResult1, maybeASubsetResult2);
        assertThat(result.result(), equalTo(SubsetResult.Result.MAYBE));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(Collections.singleton(Sets.newHashSet("abc", "def"))));
    }

    public void testMergeWhenOneIsNoAndOtherIsMaybe() {
        final SubsetResult isNotASubsetResult1 = SubsetResult.isNotASubset();
        final SubsetResult maybeASubsetResult2 = SubsetResult.mayBeASubset(Sets.newHashSet("abc", "def"));
        final SubsetResult result = SubsetResult.merge(isNotASubsetResult1, maybeASubsetResult2);
        assertThat(result.result(), equalTo(SubsetResult.Result.MAYBE));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(Collections.singleton(Sets.newHashSet("abc", "def"))));
    }

    public void testMergeWhenBothAreMaybe() {
        final SubsetResult maybeASubsetResult1 = SubsetResult.mayBeASubset(Sets.newHashSet("abc", "def"));
        final SubsetResult maybeASubsetResult2 = SubsetResult.mayBeASubset(Sets.newHashSet("ghi", "jkl"));
        final SubsetResult result = SubsetResult.merge(maybeASubsetResult1, maybeASubsetResult2);
        assertThat(result.result(), equalTo(SubsetResult.Result.MAYBE));
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("abc", "def"));
        expected.add(Sets.newHashSet("ghi", "jkl"));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(),
                equalTo(expected));
    }
}
