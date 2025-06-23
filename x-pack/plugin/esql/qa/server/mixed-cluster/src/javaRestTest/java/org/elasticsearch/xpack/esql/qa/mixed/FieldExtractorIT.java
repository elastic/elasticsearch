/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.xpack.esql.qa.rest.FieldExtractorTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FieldExtractorIT extends FieldExtractorTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    public FieldExtractorIT(MappedFieldType.FieldExtractPreference preference) {
        super(preference);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Matcher<Integer> pidMatcher() {
        // TODO these should all always return null because the parent is nested
        return preference == MappedFieldType.FieldExtractPreference.STORED ? anyOf(equalTo(111), nullValue()) : nullValue(Integer.class);
    }

    @Override
    protected void canUsePragmasOk() {
        String bwc = System.getProperty("tests.old_cluster_version");
        if (bwc == null) {
            bwc = System.getProperty("tests.serverless.bwc_stack_version");
        }
        if (bwc == null) {
            throw new AssertionError("can't find bwc version");
        }
        Version oldVersion = Version.fromString(bwc);
        assumeTrue("pragma ok not supported", oldVersion.onOrAfter("8.16.0"));
    }
}
