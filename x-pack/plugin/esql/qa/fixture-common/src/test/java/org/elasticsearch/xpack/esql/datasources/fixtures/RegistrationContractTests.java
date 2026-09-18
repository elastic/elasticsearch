/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RegistrationContractTests extends ESTestCase {

    /**
     * The real contract. Every case has to be complete, because an incomplete one is not a weaker
     * assertion -- it is a case the suite cannot run at all.
     */
    public void testTheDeclaredContractIsComplete() {
        var cases = RegistrationContract.get().cases();
        assertThat("the contract is not empty", cases.size(), greaterThan(0));
        Set<String> names = new HashSet<>();
        for (RegistrationContract.Case declared : cases) {
            assertTrue("case names are unique", names.add(declared.name()));
            assertThat("a case with no settings registers nothing", declared.settings().isEmpty(), equalTo(false));
            assertThat(declared.message().isBlank(), equalTo(false));
            assertThat(declared.emitter().isBlank(), equalTo(false));
            assertThat(declared.format().isBlank(), equalTo(false));
        }
    }

    /**
     * A message that names nothing the case pinned is the failure this file exists to prevent: it passes
     * on a failure of something the case never varied. What counts as "names" differs by outcome, and the
     * difference is real rather than a loophole -- a refusal comes from the validator and talks about the
     * setting key, while a query failure comes from the reader, which never saw a settings map and talks
     * about the file and the format it was told to expect.
     */
    public void testEveryMessageNamesSomethingTheCasePinned() {
        for (RegistrationContract.Case declared : RegistrationContract.get().cases()) {
            boolean named = switch (declared.outcome()) {
                case REFUSED -> declared.settings().keySet().stream().anyMatch(declared.message()::contains);
                case QUERY_FAILS -> {
                    String message = declared.message().toLowerCase(Locale.ROOT);
                    yield declared.settings().values().stream().anyMatch(v -> message.contains(v.toLowerCase(Locale.ROOT)));
                }
            };
            assertTrue(
                "case ["
                    + declared.name()
                    + "] ("
                    + declared.outcome()
                    + ") asserts a message naming nothing it pinned: "
                    + declared.settings(),
                named
            );
        }
    }

    /** A refusal never runs a query, so declaring one says the case was half-converted from the other kind. */
    public void testARefusedCaseMayNotDeclareAQuery() {
        Properties props = wellFormed();
        props.setProperty("case.sample_negative.query", "FROM %s | LIMIT 1");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("would never run"));
    }

    /** An unknown outcome is a typo that would otherwise silently fall back to the refusal kind. */
    public void testAnUnknownOutcomeIsRejected() {
        Properties props = wellFormed();
        props.setProperty("case.sample_negative.outcome", "maybe");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("unknown outcome [maybe]"));
    }

    /** Both halves of the no-I/O promise need a case, or the second contract has no test at all. */
    public void testTheContractCarriesBothOutcomes() {
        var outcomes = RegistrationContract.get().cases().stream().map(RegistrationContract.Case::outcome).distinct().toList();
        assertThat("both outcomes are declared", outcomes.size(), equalTo(2));
    }

    public void testAWellFormedCaseParses() {
        RegistrationContract contract = RegistrationContract.parse(wellFormed());
        assertThat(contract.cases().size(), equalTo(1));
        RegistrationContract.Case only = contract.cases().get(0);
        assertThat(only.name(), equalTo("sample_negative"));
        assertThat(only.settings(), equalTo(Map.of("schema_sample_size", "-1")));
        assertThat("the format defaults rather than being required of every case", only.format(), equalTo("csv"));
    }

    public void testAnExplicitFormatOverridesTheDefault() {
        Properties props = wellFormed();
        props.setProperty("case.sample_negative.format", "ndjson");
        assertThat(RegistrationContract.parse(props).cases().get(0).format(), equalTo("ndjson"));
    }

    /** An unknown attribute is a typo, and a typo silently drops the assertion the line was meant to make. */
    public void testAnUnknownAttributeIsRejected() {
        Properties props = wellFormed();
        props.setProperty("case.sample_negative.mesage", "typo");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("unknown attribute [mesage]"));
    }

    public void testAKeyOutsideTheCaseNamespaceIsRejected() {
        Properties props = wellFormed();
        props.setProperty("suites", "csv");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("expected 'case.<name>.<attribute>'"));
    }

    /**
     * Each required field is required on its own. A case missing one is not a case with a default -- the
     * suite would register nothing, assert nothing, and pass.
     */
    public void testEveryRequiredFieldIsRequired() {
        for (String attribute : new String[] { "message", "emitter" }) {
            Properties props = wellFormed();
            props.remove("case.sample_negative." + attribute);
            Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
            assertThat(e.getMessage(), containsString("declares no [" + attribute + "]"));
        }
    }

    /** A blank value is the same missing declaration as an absent one, and reads as deliberate. */
    public void testABlankFieldIsRefusedLikeAnAbsentOne() {
        Properties props = wellFormed();
        props.setProperty("case.sample_negative.message", "   ");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("declares no [message]"));
    }

    /** An empty contract asserts nothing and would report green having checked no refusal at all. */
    public void testAnEmptyContractIsRejected() {
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(new Properties()));
        assertThat(e.getMessage(), containsString("declares no cases"));
    }

    private static Properties wellFormed() {
        Properties props = new Properties();
        props.setProperty("case.sample_negative.settings.schema_sample_size", "-1");
        props.setProperty("case.sample_negative.message", "[schema_sample_size] must be between 1 and 20000, got [-1]");
        props.setProperty("case.sample_negative.emitter", "DataSourceValidationUtils.validateInt");
        return props;
    }

    /**
     * The refusals worth having pin two settings that are each accepted alone. A contract of one
     * setting per case cannot express one, so this pins that the shape survives.
     */
    public void testACaseMayRegisterMoreThanOneSetting() {
        Properties props = new Properties();
        props.setProperty("case.probe_budget.settings.split_probe_window", "64mb");
        props.setProperty("case.probe_budget.settings.max_split_probes", "1000");
        props.setProperty("case.probe_budget.message", "Invalid combination of [split_probe_window]");
        props.setProperty("case.probe_budget.emitter", "FileSplitProvider.validateProbeBudget");
        RegistrationContract.Case only = RegistrationContract.parse(props).cases().get(0);
        assertThat(only.settings(), equalTo(Map.of("split_probe_window", "64mb", "max_split_probes", "1000")));
    }

    /** A case that registers nothing asserts nothing about registration. */
    public void testACaseWithNoSettingsIsRejected() {
        Properties props = new Properties();
        props.setProperty("case.empty.message", "something");
        props.setProperty("case.empty.emitter", "Somewhere.method");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("declares no settings"));
    }

    /** At least one declared case pins a combination, or the contract has lost its interesting half. */
    public void testTheContractCarriesAtLeastOneCombination() {
        assertTrue(
            "no declared case registers more than one setting",
            RegistrationContract.get().cases().stream().anyMatch(c -> c.settings().size() > 1)
        );
    }
}
