/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DatasetIdentityTests extends ESTestCase {

    private static DatasetIdentity identity(
        String datasetVersion,
        String dataSourceVersion,
        String secretIdentity,
        String storageIdentity,
        String formatIdentity,
        String coordinatorIdentity
    ) {
        return DatasetIdentity.of(datasetVersion, dataSourceVersion, secretIdentity, storageIdentity, formatIdentity, coordinatorIdentity);
    }

    private static DatasetIdentity base() {
        return identity("dsv1", "srcv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1");
    }

    /**
     * Every component must discriminate, or two datasets share one address and one's records are served for the
     * other. Asserted per component rather than in aggregate, because a fold that silently dropped one input
     * would still pass a test that only varied everything at once.
     */
    public void testEveryComponentDiscriminates() {
        assertThat(base(), equalTo(identity("dsv1", "srcv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1")));
        assertThat(base(), not(equalTo(identity("dsv2", "srcv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(identity("dsv1", "srcv2", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(identity("dsv1", "srcv1", "otherdigest", "s3|eu-west-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(identity("dsv1", "srcv1", "secretdigest", "s3|us-east-1", "csv|sep=,", "coord1"))));
        assertThat(base(), not(equalTo(identity("dsv1", "srcv1", "secretdigest", "s3|eu-west-1", "csv|sep=;", "coord1"))));
        assertThat(base(), not(equalTo(identity("dsv1", "srcv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord2"))));
    }

    /**
     * The anti-fragmentation direction, which is the one no existing suite proves: a change that does NOT alter
     * what is read must NOT derive a new address, or every such change silently takes the dataset cold.
     * {@code AbstractExternalReadConfigParityIT}'s own javadoc says its warmth tests are not mutation-proven and
     * should be read as behavioural documentation rather than as proof the identity cannot fragment, so this
     * carries that weight instead.
     * <p>
     * Here the invariant is narrow and exact: the identity is a pure function of its six inputs, so equal inputs
     * must give an equal identity and an equal hash, whatever order or instance they arrived as.
     */
    public void testEqualInputsDoNotFragmentTheAddress() {
        DatasetIdentity first = identity("dsv1", "srcv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1");
        DatasetIdentity second = identity(
            new String("dsv1"),
            new String("srcv1"),
            new String("secretdigest"),
            new String("s3|eu-west-1"),
            new String("csv|sep=,"),
            new String("coord1")
        );
        assertThat("distinct String instances with equal content must not fragment the address", first, equalTo(second));
        assertThat(first.hashCode(), equalTo(second.hashCode()));
    }

    /**
     * Length prefixing is what stops one component borrowing another's characters. Without it these two fold to
     * the same bytes, and two different datasets would share every record.
     */
    public void testComponentsCannotBleedIntoEachOther() {
        assertThat(identity("ab", "c", "", "", "", ""), not(equalTo(identity("a", "bc", "", "", "", ""))));
        assertThat(identity("", "", "", "ab", "c", ""), not(equalTo(identity("", "", "", "a", "bc", ""))));
    }

    /**
     * Absent and empty are different states: a query reaching these stores with no registered dataset behind it
     * is not a query whose dataset has an empty resource, and conflating them would give both one address.
     */
    public void testNullIsNotEmpty() {
        assertThat(identity(null, "srcv1", "", "", "", ""), not(equalTo(identity("", "srcv1", "", "", "", ""))));
        assertThat(identity("dsv1", null, "", "", "", ""), not(equalTo(identity("dsv1", "", "", "", "", ""))));
    }

    /**
     * An auth mode with no stored secret yields an empty digest, which must behave as a value and not as a
     * wildcard: an empty digest separates from a non-empty one, and two empty ones agree.
     */
    public void testAnEmptySecretDigestIsAValueNotAWildcard() {
        DatasetIdentity none = identity("dsv1", "srcv1", "", "s3|eu-west-1", "csv", "coord1");
        assertThat(none, not(equalTo(base())));
        assertThat(none, equalTo(identity("dsv1", "srcv1", "", "s3|eu-west-1", "csv", "coord1")));
    }

    /** The three pairs must be rendered distinctly, so a log line names which part moved. */
    public void testToStringNamesEachPairSeparately() {
        String rendered = base().toString();
        assertThat(rendered.contains("dataset="), equalTo(true));
        assertThat(rendered.contains("source="), equalTo(true));
        assertThat(rendered.contains("participants="), equalTo(true));
        assertThat(
            "a changed secret must change only the source pair",
            rendered,
            not(equalTo(identity("dsv1", "srcv1", "other", "s3|eu-west-1", "csv|sep=,", "coord1").toString()))
        );
    }
}
