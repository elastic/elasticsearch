/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(RandomizedRunner.class)
public class AnalyzerAmbiguityTests extends ESTestCase {

    // Reflection：private static void checkAmbiguousUnqualifiedName(UnresolvedAttribute, List<Attribute>)
    private static Method checkMethod() throws NoSuchMethodException {
        Method m = Analyzer.class.getDeclaredMethod(
            "checkAmbiguousUnqualifiedName", UnresolvedAttribute.class, List.class
        );
        m.setAccessible(true);
        return m;
    }

    private static UnresolvedAttribute ua(String name, String qualifierOrNull) {
        UnresolvedAttribute ua = mock(UnresolvedAttribute.class);
        when(ua.name()).thenReturn(name);
        when(ua.qualifier()).thenReturn(qualifierOrNull);
        return ua;
    }

    private static Attribute attr(String name, String qualifierOrNull) {
        Attribute a = mock(Attribute.class);
        when(a.name()).thenReturn(name);
        when(a.qualifier()).thenReturn(qualifierOrNull);
        return a;
    }

    @Test
    public void testAmbiguousUnqualifiedThrowsWhenQualifiersExist() throws Exception {
        var attrs = Arrays.asList(attr("id","a"), attr("id","b"), attr("name","a"));
        var id = ua("id", null);
        try {
            checkMethod().invoke(null, id, attrs);
            fail("Expected VerificationException due to ambiguous unqualified name 'id'");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            assertTrue("Expected VerificationException, got: " + cause,
                cause instanceof VerificationException);
            assertTrue(cause.getMessage(), cause.getMessage().contains("Ambiguous unqualified name [id]"));
            assertTrue(cause.getMessage(), cause.getMessage().contains("[a].[id]"));
            assertTrue(cause.getMessage(), cause.getMessage().contains("[b].[id]"));
        }
    }

    @Test
    public void testQualifiedNameBypassesAmbiguityCheck() throws Exception {
        var attrs = Arrays.asList(attr("id","a"), attr("id","b"));
        var qualified = ua("id","a");
        checkMethod().invoke(null, qualified, attrs); // Should not throw
    }

    @Test
    public void testNoQualifiersButDuplicateNamesShouldThrow() throws Exception {
        // All attributes have a null qualifier, but there are duplicate names -> still ambiguous
        var attrs = Arrays.asList(
            attr("id", null),
            attr("id", null),
            attr("name", null)
        );
        var id = ua("id", null); // unqualified reference

        // Call resolveAgainstList here; it now checks ambiguity for any unqualified ref with duplicate candidates
        Method m = Analyzer.class.getDeclaredMethod(
            "resolveAgainstList", UnresolvedAttribute.class, java.util.Collection.class
        );
        m.setAccessible(true);

        try {
            m.invoke(null, id, attrs);
            fail("Expected VerificationException due to ambiguous unqualified name 'id' (even when qualifiers are null)");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            assertTrue("Expected VerificationException, got: " + cause,
                cause instanceof VerificationException);
            assertTrue(cause.getMessage().contains("Ambiguous unqualified name [id]"));
        }
    }
}
