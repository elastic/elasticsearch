/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DistinguishedNameNormalizerTests extends ESTestCase {

    private UserRoleMapper.DistinguishedNameNormalizer dnNormalizer;

    @Before
    public void init() {
        dnNormalizer = getDnNormalizer();
    }

    public void testDnNormalizingIsCached() {
        // Parse same DN multiple times, only 1st time DN parsing is performed, 2nd time reads from the cache
        Mockito.clearInvocations(dnNormalizer);
        final String dn = randomDn();
        parseDnMultipleTimes(dn);
        verify(dnNormalizer, times(1)).doNormalize(dn);

        // The cache is keyed by the literal string form.
        // Therefore if the literal string changes, it needs to be parsed again even though it is still the same DN
        Mockito.clearInvocations(dnNormalizer);
        final String mutatedDn = mutateDn(dn);
        parseDnMultipleTimes(mutatedDn);
        verify(dnNormalizer, times(1)).doNormalize(mutatedDn);

        // Invalid DNs should also be cached
        Mockito.clearInvocations(dnNormalizer);
        final String invalidDn = randomFrom(
            "",
            randomAlphaOfLengthBetween(1, 8),
            randomAlphaOfLengthBetween(1, 8) + "*",
            randomAlphaOfLengthBetween(1, 8) + "=",
            "=" + randomAlphaOfLengthBetween(1, 8)
        );
        parseDnMultipleTimes(invalidDn);
        verify(dnNormalizer, times(1)).doNormalize(invalidDn);
    }

    public void testDnNormalizingIsCachedForDnPredicate() {
        final String dn = randomDn();
        final Predicate<FieldValue> predicate = new UserRoleMapper.DistinguishedNamePredicate(dn, dnNormalizer);
        verify(dnNormalizer, times(1)).doNormalize(dn);

        // Same DN, it's cached
        runPredicateMultipleTimes(predicate, dn);
        verify(dnNormalizer, times(1)).doNormalize(dn);

        // Predicate short-circuits for case differences
        Mockito.clearInvocations(dnNormalizer);
        final String casedDn = randomFrom(dn.toLowerCase(Locale.ENGLISH), dn.toUpperCase(Locale.ENGLISH));
        runPredicateMultipleTimes(predicate, casedDn);
        verify(dnNormalizer, never()).doNormalize(anyString());

        // Literal string form changes, it will be parsed again
        Mockito.clearInvocations(dnNormalizer);
        final String mutatedDn = randomFrom(dn.replace(" ", ""), dn.replace(",", " ,"));
        runPredicateMultipleTimes(predicate, mutatedDn);
        verify(dnNormalizer, times(1)).doNormalize(mutatedDn);

        // Subtree DN is also cached
        Mockito.clearInvocations(dnNormalizer);
        final String subtreeDn = "*," + randomDn();
        runPredicateMultipleTimes(predicate, subtreeDn);
        verify(dnNormalizer, times(1)).doNormalize(subtreeDn.substring(2));

        // Subtree DN is also keyed by the literal form, so they are space sensitive
        Mockito.clearInvocations(dnNormalizer);
        final String mutatedSubtreeDn = "*, " + subtreeDn.substring(2);
        runPredicateMultipleTimes(predicate, mutatedSubtreeDn);
        verify(dnNormalizer, times(1)).doNormalize(mutatedSubtreeDn.substring(2));
    }

    public void testUserDataUsesCachedDnNormalizer() {
        final String userDn = "uid=foo," + randomDn();
        final List<String> groups = IntStream.range(0, randomIntBetween(50, 100))
            .mapToObj(i -> "gid=g" + i + "," + randomDn())
            .distinct()
            .toList();
        final RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.name()).thenReturn(randomAlphaOfLengthBetween(3, 8));
        final UserRoleMapper.UserData userData = new UserRoleMapper.UserData(
            randomAlphaOfLengthBetween(5, 8),
            userDn,
            groups,
            Map.of(),
            realmConfig
        );
        UserRoleMapper.UserData spyUserdata = spy(userData);
        final UserRoleMapper.DistinguishedNameNormalizer spyDnNormalizer = spy(userData.getDnNormalizer());
        when(spyUserdata.getDnNormalizer()).thenReturn(spyDnNormalizer);

        final ExpressionModel expressionModel = spyUserdata.asModel();

        // All DNs to be tested should only be parsed once no matter how many groups the userData may have
        Mockito.clearInvocations(spyDnNormalizer);
        final List<String> dnList = randomList(100, 200, DistinguishedNameNormalizerTests::randomDn).stream().distinct().toList();
        final List<FieldValue> fieldValues = dnList.stream()
            .map(dn -> randomBoolean() ? new FieldValue(dn) : new FieldValue("*," + dn))
            .toList();
        expressionModel.test("groups", fieldValues);
        // Also does not matter how many times the model is tested
        expressionModel.test("groups", randomNonEmptySubsetOf(fieldValues));

        final ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(spyDnNormalizer, times(dnList.size())).doNormalize(argumentCaptor.capture());
        assertThat(argumentCaptor.getAllValues(), equalTo(dnList));
    }

    private void parseDnMultipleTimes(String dn) {
        IntStream.range(0, randomIntBetween(3, 5)).forEach(i -> dnNormalizer.normalize(dn));
    }

    private void runPredicateMultipleTimes(Predicate<FieldValue> predicate, Object value) {
        IntStream.range(0, randomIntBetween(3, 5)).forEach(i -> predicate.test(new FieldValue(value)));
    }

    private UserRoleMapper.DistinguishedNameNormalizer getDnNormalizer() {
        return spy(new UserRoleMapper.DistinguishedNameNormalizer());
    }

    private static String randomDn() {
        return "CN="
            + randomAlphaOfLengthBetween(3, 12)
            + ",OU="
            + randomAlphaOfLength(4)
            + ", O="
            + randomAlphaOfLengthBetween(2, 6)
            + ",dc="
            + randomAlphaOfLength(3);
    }

    private static String mutateDn(String dn) {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> dn.toLowerCase(Locale.ENGLISH);
            case 2 -> dn.toUpperCase(Locale.ENGLISH);
            case 3 -> dn.replace(" ", "");
            default -> dn.replace(",", " ,");
        };
    }
}
