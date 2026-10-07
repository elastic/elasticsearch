/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DatasetIdentityTests extends ESTestCase {

    private static DatasetIdentity base() {
        return DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1");
    }

    /**
     * Every component must discriminate, or two datasets share one address and one's records answer for the other.
     * Asserted one component at a time: a fold that silently dropped an input would still pass a case that varied
     * everything at once.
     */
    public void testEveryComponentDiscriminates() {
        assertThat(base(), equalTo(DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1")));
        assertThat(base(), not(equalTo(DatasetIdentity.of("defv2", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(DatasetIdentity.of("defv1", "otherdigest", "s3|eu-west-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(DatasetIdentity.of("defv1", "secretdigest", "s3|us-east-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv|sep=;", "coord1"))));
        assertThat(base(), not(equalTo(DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord2"))));
    }

    /**
     * The anti-fragmentation direction, which no existing suite proves: inputs that agree must NOT derive a new
     * address, or every resolve takes the dataset cold. {@code AbstractExternalReadConfigParityIT}'s own javadoc says
     * its warmth cases are behavioural documentation rather than proof the identity cannot fragment, so this carries
     * that weight instead. The invariant is exact: the identity is a pure function of its five inputs.
     */
    public void testEqualInputsDoNotFragmentTheAddress() {
        DatasetIdentity first = base();
        DatasetIdentity second = DatasetIdentity.of(
            new String("defv1"),
            new String("secretdigest"),
            new String("s3|eu-west-1"),
            new String("csv|sep=,"),
            new String("coord1")
        );
        assertThat("distinct String instances with equal content must not fragment the address", first, equalTo(second));
        assertThat(first.hashCode(), equalTo(second.hashCode()));
    }

    /** Length prefixing stops one component borrowing another's characters; without it these fold to the same bytes. */
    public void testComponentsCannotBleedIntoEachOther() {
        assertThat(DatasetIdentity.of("", "", "ab", "c", ""), not(equalTo(DatasetIdentity.of("", "", "a", "bc", ""))));
        assertThat(DatasetIdentity.of("", "", "", "ab", "c"), not(equalTo(DatasetIdentity.of("", "", "", "a", "bc"))));
    }

    /**
     * Absent and empty are different states: a query reaching these stores with no registered dataset behind it is not
     * a query whose dataset has an empty definition version, and conflating them gives both one address.
     */
    public void testNullIsNotEmpty() {
        assertThat(DatasetIdentity.of(null, "d", "", "", ""), not(equalTo(DatasetIdentity.of("", "d", "", "", ""))));
        assertThat(DatasetIdentity.of("v", null, "", "", ""), not(equalTo(DatasetIdentity.of("v", "", "", "", ""))));
    }

    /**
     * An auth mode with no stored secret yields an empty digest, which must behave as a value and not a wildcard:
     * empty separates from non-empty, and two empties agree.
     */
    public void testAnEmptySecretDigestIsAValueNotAWildcard() {
        DatasetIdentity none = DatasetIdentity.of("defv1", "", "s3|eu-west-1", "csv", "coord1");
        assertThat(none, not(equalTo(DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv", "coord1"))));
        assertThat(none, equalTo(DatasetIdentity.of("defv1", "", "s3|eu-west-1", "csv", "coord1")));
    }

    /** The three lanes render separately, so a log line names which part moved. */
    public void testToStringNamesEachLaneSeparately() {
        String rendered = base().toString();
        assertThat(rendered.contains("dataset="), equalTo(true));
        assertThat(rendered.contains("source="), equalTo(true));
        assertThat(rendered.contains("participants="), equalTo(true));
    }

    public void testDefinitionVersionOfReadsTheConfigOrAnswersEmpty() {
        assertThat(DatasetIdentity.definitionVersionOf(Map.of(DefinitionVersion.CONFIG_KEY, "7")), equalTo("7"));
        assertThat(DatasetIdentity.definitionVersionOf(Map.of()), equalTo(""));
        assertThat(DatasetIdentity.definitionVersionOf(null), equalTo(""));
        // A non-String under the key is a query with no usable version, not a reason to render one.
        assertThat(DatasetIdentity.definitionVersionOf(Map.of(DefinitionVersion.CONFIG_KEY, 7)), equalTo(""));
    }
}
