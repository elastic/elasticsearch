/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntoTargetTests extends ESTestCase {

    public void testSplitsARollup() {
        var target = IntoTarget.split(
            "FROM metrics-mv | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 minute, `@timestamp`) | INTO mv-cpu"
        );
        assertTrue("INTO should have been detected", target.isPresent());
        assertThat(target.get().destination(), equalTo("mv-cpu"));
        assertThat(
            target.get().producer(),
            equalTo("FROM metrics-mv | STATS max_cpu = MAX(cpu) BY host, `@timestamp` = DATE_TRUNC(1 minute, `@timestamp`)")
        );
    }

    public void testOrdinaryQueryHasNoTarget() {
        assertFalse(IntoTarget.split("FROM metrics-mv | STATS c = COUNT(*) BY host").isPresent());
    }

    /** The word appearing in a value says nothing about where the rows go. */
    public void testTheWordInAStringIsNotACommand() {
        assertFalse(IntoTarget.split("FROM metrics-mv | WHERE message == \"into\"").isPresent());
    }

    /**
     * The split is made by finding the command's own text in the query, so a query that also contains that text
     * somewhere harmless has to end up split at the command rather than at the decoy.
     */
    public void testACommandLikeStringInTheQueryDoesNotConfuseTheSplit() {
        var target = IntoTarget.split("FROM metrics-mv | EVAL note = \"| INTO decoy\" | INTO mv-cpu");
        assertTrue("INTO should have been detected", target.isPresent());
        assertThat(target.get().destination(), equalTo("mv-cpu"));
        assertThat(target.get().producer(), equalTo("FROM metrics-mv | EVAL note = \"| INTO decoy\""));
    }

    /** The command is not case sensitive, and neither is finding it. */
    public void testLowerCaseCommand() {
        var target = IntoTarget.split("FROM metrics-mv | STATS c = COUNT(*) BY host | into mv-cpu");
        assertTrue("INTO should have been detected", target.isPresent());
        assertThat(target.get().destination(), equalTo("mv-cpu"));
        assertThat(target.get().producer(), equalTo("FROM metrics-mv | STATS c = COUNT(*) BY host"));
    }

    /** A sort the destination cannot keep is refused rather than attempted over the whole result. */
    public void testASortWithNoLimitIsRefused() {
        final Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> IntoTarget.split("FROM metrics-mv | SORT host | INTO mv-cpu")
        );
        assertThat(e.getMessage(), containsString("SORT has no effect on what INTO writes"));
    }

    /** With a limit the sort is bounded, so it stands. */
    public void testASortWithALimitIsAccepted() {
        var target = IntoTarget.split("FROM metrics-mv | SORT host | LIMIT 10 | INTO mv-cpu");
        assertTrue("INTO should have been detected", target.isPresent());
        assertThat(target.get().producer(), equalTo("FROM metrics-mv | SORT host | LIMIT 10"));
    }

    /** A destination with no query before it does not parse, and is left to the ordinary path to report. */
    public void testADestinationOnItsOwnIsNotSplit() {
        assertFalse(IntoTarget.split("INTO mv-cpu").isPresent());
    }

    /** A query that does not parse is left for the ordinary path, which reports the error with its position. */
    public void testUnparseableQueryIsLeftAlone() {
        assertFalse(IntoTarget.split("FROM metrics-mv | STATS INTO mv-cpu").isPresent());
    }
}
