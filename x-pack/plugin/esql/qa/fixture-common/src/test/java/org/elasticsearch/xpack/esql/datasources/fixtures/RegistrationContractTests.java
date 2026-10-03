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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

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
            assertThat(declared.format().isBlank(), equalTo(false));
            // What a case must carry depends on what it asserts. A failing one is complete when it has
            // the message and the symbol that emits it; a succeeding one when it has the result shape,
            // because a message is not what distinguishes it from a setting that was ignored.
            if (declared.outcome() == RegistrationContract.Outcome.QUERY_SUCCEEDS) {
                assertThat("a succeeding case names no columns", declared.columns().isEmpty(), equalTo(false));
            } else {
                assertThat(declared.message().isBlank(), equalTo(false));
                assertThat(declared.emitter().isBlank(), equalTo(false));
            }
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
            // A case may name the setting in its NEGATIVE assertion instead: the refusal it asserts is
            // about another input, and what it pins about this setting is that the response stops
            // mentioning it. That is still naming what the case pinned.
            String asserted = declared.message() + (declared.absent() == null ? "" : " " + declared.absent());
            boolean named = switch (declared.outcome()) {
                case REFUSED -> declared.settings().keySet().stream().anyMatch(asserted::contains);
                // A succeeding case pins a result SHAPE rather than a message: the columns are what
                // distinguish a setting that took effect from one that was accepted and ignored, since
                // both return rows.
                case QUERY_SUCCEEDS -> declared.columns().isEmpty() == false;
                case QUERY_FAILS -> {
                    String message = asserted.toLowerCase(Locale.ROOT);
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
        props.setProperty("case.dataset.setting.sample.negative.query", "FROM %s | LIMIT 1");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("would never run"));
    }

    /** An unknown outcome is a typo that would otherwise silently fall back to the refusal kind. */
    public void testAnUnknownOutcomeIsRejected() {
        Properties props = wellFormed();
        props.setProperty("case.dataset.setting.sample.negative.outcome", "maybe");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("unknown outcome [maybe]"));
    }

    /**
     * Every outcome has at least one case. An outcome nobody declares is a contract with no test at
     * all, and it is the kind of gap that reads as covered because the other outcomes are green.
     */
    public void testEveryOutcomeHasACase() {
        var declared = RegistrationContract.get().cases().stream().map(RegistrationContract.Case::outcome).collect(Collectors.toSet());
        assertThat(declared, equalTo(Set.of(RegistrationContract.Outcome.values())));
    }

    public void testAWellFormedCaseParses() {
        RegistrationContract contract = RegistrationContract.parse(wellFormed());
        assertThat(contract.cases().size(), equalTo(1));
        RegistrationContract.Case only = contract.cases().get(0);
        assertThat(only.name(), equalTo("dataset.setting.sample.negative"));
        assertThat(only.settings(), equalTo(Map.of("schema_sample_size", "-1")));
        assertThat("the format defaults rather than being required of every case", only.format(), equalTo("csv"));
    }

    public void testAnExplicitFormatOverridesTheDefault() {
        Properties props = wellFormed();
        props.setProperty("case.dataset.setting.sample.negative.format", "ndjson");
        assertThat(RegistrationContract.parse(props).cases().get(0).format(), equalTo("ndjson"));
    }

    /** An unknown attribute is a typo, and a typo silently drops the assertion the line was meant to make. */
    public void testAnUnknownAttributeIsRejected() {
        Properties props = wellFormed();
        props.setProperty("case.dataset.setting.sample.negative.mesage", "typo");
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
            props.remove("case.dataset.setting.sample.negative." + attribute);
            Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
            assertThat(e.getMessage(), containsString("declares no [" + attribute + "]"));
        }
    }

    /** A blank value is the same missing declaration as an absent one, and reads as deliberate. */
    public void testABlankFieldIsRefusedLikeAnAbsentOne() {
        Properties props = wellFormed();
        props.setProperty("case.dataset.setting.sample.negative.message", "   ");
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
        props.setProperty("case.dataset.setting.sample.negative.settings.schema_sample_size", "-1");
        props.setProperty("case.dataset.setting.sample.negative.message", "[schema_sample_size] must be between 1 and 20000, got [-1]");
        props.setProperty("case.dataset.setting.sample.negative.emitter", "DataSourceValidationUtils.validateInt");
        return props;
    }

    /**
     * The refusals worth having pin two settings that are each accepted alone. A contract of one
     * setting per case cannot express one, so this pins that the shape survives.
     */
    public void testACaseMayRegisterMoreThanOneSetting() {
        Properties props = new Properties();
        props.setProperty("case.dataset.combination.probe_budget.settings.split_probe_window", "64mb");
        props.setProperty("case.dataset.combination.probe_budget.settings.max_split_probes", "1000");
        props.setProperty("case.dataset.combination.probe_budget.message", "Invalid combination of [split_probe_window]");
        props.setProperty("case.dataset.combination.probe_budget.emitter", "FileSplitProvider.validateProbeBudget");
        RegistrationContract.Case only = RegistrationContract.parse(props).cases().get(0);
        assertThat(only.settings(), equalTo(Map.of("split_probe_window", "64mb", "max_split_probes", "1000")));
    }

    /** A case that registers nothing asserts nothing about registration. */
    public void testACaseWithNoSettingsIsRejected() {
        Properties props = new Properties();
        props.setProperty("case.dataset.setting.empty.message", "something");
        props.setProperty("case.dataset.setting.empty.emitter", "Somewhere.method");
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

    /**
     * A blocked case asserts behaviour the product does not have. Without the issue nothing closes the
     * exclusion when the defect is fixed, so the case sits skipped long after the reason is gone --
     * which loses exactly the coverage it was written to add.
     */
    public void testABlockedCaseMustCiteTheIssueThatWillUnblockIt() {
        Properties props = wellFormed();
        props.setProperty("case.dataset.setting.sample.negative.blocked_by", "it is broken");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("cites no issue"));

        props.setProperty("case.dataset.setting.sample.negative.blocked_by", "elastic/esql-planning#1999");
        assertThat(RegistrationContract.parse(props).cases().get(0).blocked(), equalTo(true));
    }

    /** Every blocked case in the real contract carries its issue, so none of them can be forgotten. */
    public void testEveryBlockedCaseInTheContractCitesAnIssue() {
        for (RegistrationContract.Case declared : RegistrationContract.get().cases()) {
            if (declared.blocked()) {
                assertTrue(
                    "case [" + declared.name() + "] is blocked on [" + declared.blockedBy() + "], which names no filed issue",
                    RegistrationContract.ISSUE_REFERENCE.matcher(declared.blockedBy()).find()
                );
            }
        }
    }

    /**
     * The negative assertion is the whole point of some cases: the failure happens either way, and what
     * must change is that a second, misleading error stops coming with it.
     */
    public void testACaseMayAssertASubstringIsAbsent() {
        Properties props = wellFormed();
        props.setProperty("case.dataset.setting.sample.negative.absent", "unknown setting");
        assertThat(RegistrationContract.parse(props).cases().get(0).absent(), equalTo("unknown setting"));
        assertThat("absent is optional", RegistrationContract.parse(wellFormed()).cases().get(0).absent(), nullValue());
    }

    /**
     * A resource is registered as a URI unless the case says otherwise. Writing a bare path is the user
     * mistake some cases are about, so the form is declared rather than guessed from the string.
     */
    public void testTheResourceFormIsDeclaredAndValidated() {
        Properties props = wellFormed();
        assertThat("uri by default", RegistrationContract.parse(props).cases().get(0).rawPath(), equalTo(false));
        props.setProperty("case.dataset.setting.sample.negative.resource_form", "path");
        assertThat(RegistrationContract.parse(props).cases().get(0).rawPath(), equalTo(true));
        props.setProperty("case.dataset.setting.sample.negative.resource_form", "sideways");
        Exception e = expectThrows(IllegalStateException.class, () -> RegistrationContract.parse(props));
        assertThat(e.getMessage(), containsString("expected [uri] or [path]"));
    }
}
