/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;

public class IndexDeprecationChecksTests extends ESTestCase {

    public void testOldIndicesCheck() {
        Version createdWith = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            VersionUtils.getPreviousVersion(Version.V_6_0_0_alpha1));
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(settings(createdWith))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Index created before 6.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                "breaking-changes-7.0.html",
            "this index was created using version: " + createdWith);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }

    public void testDelimitedPayloadFilterCheck() {
        Settings settings = settings(
            VersionUtils.randomVersionBetween(random(), Version.V_6_0_0_alpha1, VersionUtils.getPreviousVersion(Version.CURRENT)))
            .put("index.analysis.filter.my_delimited_payload_filter.type", "delimited_payload_filter")
            .put("index.analysis.filter.my_delimited_payload_filter.delimiter", "^")
            .put("index.analysis.filter.my_delimited_payload_filter.encoding", "identity").build();
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING, "Use of 'delimited_payload_filter'.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_analysis_changes.html",
            "[The filter [my_delimited_payload_filter] is of deprecated 'delimited_payload_filter' type. "
                + "The filter type should be changed to 'delimited_payload'.]");
        List<DeprecationIssue> issues = DeprecationInfoAction.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertEquals(singletonList(expected), issues);
    }
}
