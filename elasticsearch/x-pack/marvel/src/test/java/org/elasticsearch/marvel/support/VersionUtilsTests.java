/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.support;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class VersionUtilsTests extends ESTestCase {

    public void testParseVersion() {
        List<Version> versions = randomSubsetOf(9, Version.V_2_0_0_beta1, Version.V_2_0_0_beta2, Version.V_2_0_0_rc1, Version.V_2_0_0,
                Version.V_2_0_1, Version.V_2_0_2, Version.V_2_1_0, Version.V_2_1_1, Version.V_2_1_2, Version.V_2_2_0, Version.V_2_3_0,
                Version.V_5_0_0_alpha1);
        for (Version version : versions) {
            String output = createOutput(VersionUtils.VERSION_NUMBER_FIELD, version.toString());
            assertThat(VersionUtils.parseVersion(output.getBytes(StandardCharsets.UTF_8)), equalTo(version));
            assertThat(VersionUtils.parseVersion(VersionUtils.VERSION_NUMBER_FIELD, output), equalTo(version));
        }
    }

    private String createOutput(String fieldName, String value) {
        return "{\n" +
                "  \"name\" : \"Blind Faith\",\n" +
                "  \"cluster_name\" : \"elasticsearch\",\n" +
                "  \"version\" : {\n" +
                "    \"" + fieldName + "\" : \"" + value + "\",\n" +
                "    \"build_hash\" : \"4092d253dddda0ff1ff3d1c09ac7678e757843f9\",\n" +
                "    \"build_timestamp\" : \"2015-10-13T08:53:10Z\",\n" +
                "    \"build_snapshot\" : true,\n" +
                "    \"lucene_version\" : \"5.2.1\"\n" +
                "  },\n" +
                "  \"tagline\" : \"You Know, for Search\"\n" +
                "}\n";
    }
}
