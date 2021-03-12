/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.painless.ScriptTestCase;
import org.elasticsearch.painless.action.PainlessSuggest.Suggestion;

import java.util.List;

public class SuggestTests extends ScriptTestCase {

    private static void compareSuggestions(List<Suggestion> actual, String... values) {
        assertEquals(values.length % 2, 0);
        assertEquals(actual.size(), values.length / 2);

        for (int i = 0; i < values.length; ++i) {
            assertTrue(actual.contains(new Suggestion(values[i], values[++i])));
        }
    }

    public void testVariables() {
        compareSuggestions(
                suggest("List test; tes"),
                Suggestion.VARIABLE, "test"
        );

        compareSuggestions(suggest("List test0, test1; int teaser; te"),
                Suggestion.VARIABLE, "test0",
                Suggestion.VARIABLE, "test1",
                Suggestion.VARIABLE, "teaser"
        );

        compareSuggestions(
                suggest("List test0, test1; if (condition) { int teaser; } return te"),
                Suggestion.VARIABLE, "test0",
                Suggestion.VARIABLE, "test1"
        );

        compareSuggestions(
                suggest("List test0, test1; if (condition) { int teaser; return te"),
                Suggestion.VARIABLE, "test0",
                Suggestion.VARIABLE, "test1",
                Suggestion.VARIABLE, "teaser"
        );

        compareSuggestions(
                suggest("List test0, test1; if (condition) { int teaser; } else return te"),
                Suggestion.VARIABLE, "test0",
                Suggestion.VARIABLE, "test1"
        );

        compareSuggestions(
                suggest("List test0, test1; if (condition) if (condition) { int teaser; } else return te"),
                Suggestion.VARIABLE, "test0",
                Suggestion.VARIABLE, "test1"
        );
    }

    public void testMethods() {
        compareSuggestions(
                suggest("GeoPoint test; test."),
                Suggestion.METHOD, "getLat/0",
                Suggestion.METHOD, "getLon/0",
                Suggestion.METHOD, "hashCode/0",
                Suggestion.METHOD, "equals/1",
                Suggestion.METHOD, "toString/0"
        );

        compareSuggestions(
                suggest("List list; list.add"),
                Suggestion.METHOD, "add/1",
                Suggestion.METHOD, "add/2",
                Suggestion.METHOD, "addAll/1",
                Suggestion.METHOD, "addAll/2"
        );

        compareSuggestions(
                suggest("Math.ma"),
                Suggestion.METHOD, "max/2"
        );

        compareSuggestions(
                suggest("int value = Math.max(2.0, 2.0), Math.min(va"),
                Suggestion.VARIABLE, "value"
        );
    }

    public void testFields() {
        compareSuggestions(
                suggest("Math.P"),
                Suggestion.FIELD, "PI"
        );
    }

    public void testFunctions() {
        compareSuggestions(
                suggest("para"),
                Suggestion.VARIABLE, "params"
        );

        compareSuggestions(
                suggest("int testF() {} int testY(blah) {} test"),
                Suggestion.USER, "testF/0",
                Suggestion.USER, "testY/0"
        );

        compareSuggestions(
                suggest("int testF() {} int testY(blah) {} test"),
                Suggestion.USER, "testF/0",
                Suggestion.USER, "testY/0"
        );

        compareSuggestions(
                suggest("int testF() {} int testY(blah) {} testY().toS"),
                Suggestion.METHOD, "toString/0"
        );

        compareSuggestions(
                suggest("int testF() {} int testY(List x) {return blah blah} test"),
                Suggestion.USER, "testF/0",
                Suggestion.USER, "testY/1"
        );

        compareSuggestions(
                suggest("int testF() {} int testY(blah) {trash trash} testY().toS"),
                Suggestion.METHOD, "toString/0"
        );

        compareSuggestions(
                suggest("int testF(int para) {par"),
                Suggestion.VARIABLE, "para"
        );

        compareSuggestions(
                suggest("int testF(int para) {int par = para; return par} par"),
                Suggestion.VARIABLE, "params"
        );
    }
}
