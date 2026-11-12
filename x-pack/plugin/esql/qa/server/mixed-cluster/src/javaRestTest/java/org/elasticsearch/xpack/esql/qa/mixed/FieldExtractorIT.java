/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceMixedClusterTestSupport;
import org.elasticsearch.xpack.esql.qa.rest.FieldExtractorTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.util.List;
import java.util.regex.Pattern;

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
        // Older nodes can still load nested pid from _source in STORED mode.
        return preference == MappedFieldType.FieldExtractPreference.STORED ? anyOf(equalTo(111), nullValue()) : nullValue(Integer.class);
    }

    @Override
    protected AssertWarnings withoutKeysWarnings() {
        if (EsqlDataSourceMixedClusterTestSupport.bwcVersion().onOrAfter(Version.V_9_4_0)) {
            return super.withoutKeysWarnings();
        }
        // Nodes before 9.4 have no loader for a name under a flattened field, and warn that they read null for it
        return new AssertWarnings.AllowedRegexes(
            List.of(
                Pattern.compile(Pattern.quote("Field [attrs.bar] cannot be retrieved, it is unsupported or not indexed; returning null"))
            )
        );
    }

    @Override
    protected void canUsePragmasOk() {
        assumeTrue(
            "pragma ok not supported",
            EsqlDataSourceMixedClusterTestSupport.bwcVersion().onOrAfter(org.elasticsearch.Version.V_8_16_0)
        );
    }
}
