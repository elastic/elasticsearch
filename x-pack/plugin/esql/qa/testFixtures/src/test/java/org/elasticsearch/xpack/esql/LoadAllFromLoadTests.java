/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.net.URL;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

public class LoadAllFromLoadTests extends ESTestCase {

    /**
     * The suites that run the {@code unmapped_fields="load"} queries again with {@code "LOAD_ALL"} generate a class for each spec file in
     * {@link LoadAllFromLoad#SPEC_FILES}, and the {@code build.gradle} files of the single-node and multi-node suites list the same files.
     * A spec file with such a query that is not listed would not be run with {@code LOAD_ALL}, with nothing to say so, so this fails
     * when the list and the spec files disagree.
     */
    public void testSpecFilesAreTheSpecFilesWithAQueryThatLoads() throws Exception {
        Set<String> withQueryThatLoads = new TreeSet<>();
        for (URL url : EsqlTestUtils.classpathResources("/*.csv-spec")) {
            for (Object[] spec : SpecReader.readURLSpec(url, CsvSpecReader.specParser())) {
                if (LoadAllFromLoad.usesLoad(((CsvSpecReader.CsvTestCase) spec[4]).query)) {
                    withQueryThatLoads.add((String) spec[0]);
                }
            }
        }
        assertThat(
            "Keep LoadAllFromLoad.SPEC_FILES, and the EsqlSpecLoadAll variant in the build.gradle of the single-node and multi-node suites, "
                + "in line with the spec files that have a query with unmapped_fields=\"load\"",
            new TreeSet<>(Arrays.asList(LoadAllFromLoad.SPEC_FILES)),
            equalTo(withQueryThatLoads)
        );
    }
}
