/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.randomsampling;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RandomSamplingQueryBuilderTests extends AbstractQueryTestCase<RandomSamplingQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MockLicensedAnalyticsPlugin.class);
    }

    public static class MockLicensedAnalyticsPlugin extends AnalyticsPlugin {

        public MockLicensedAnalyticsPlugin() {
            super();
        }

        @Override
        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(Settings.EMPTY) {
                @Override
                public boolean isAllowedByLicense(License.OperationMode minimumMode) {
                    return minimumMode.equals(License.OperationMode.PLATINUM);
                }
            };
        }
    }

    private boolean isCacheable = false;

    @Override
    protected RandomSamplingQueryBuilder doCreateTestQueryBuilder() {
        double p = randomDoubleBetween(0.00001, 0.999999, true);
        RandomSamplingQueryBuilder builder = new RandomSamplingQueryBuilder(p);
        if (randomBoolean()) {
            builder.setSeed(123);
            isCacheable = true;
        }
        return builder;
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return isCacheable;
    }

    @Override
    protected void doAssertLuceneQuery(RandomSamplingQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(RandomSamplingQuery.class));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder(0.0));
        assertEquals("[probability] cannot be less than or equal to 0.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder(-5.0));
        assertEquals("[probability] cannot be less than or equal to 0.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder(1.0));
        assertEquals("[probability] cannot be greater than or equal to 1.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder(5.0));
        assertEquals("[probability] cannot be greater than or equal to 1.0.", e.getMessage());
    }


    public void testFromJson() throws IOException {
        String json =
            "{\n" +
                "  \"random_sample\" : {\n" +
                "    \"boost\" : 1.0,\n" +
                "    \"probability\" : 0.5\n" +
                "  }\n" +
                "}";
        RandomSamplingQueryBuilder parsed = (RandomSamplingQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), nullValue());

        // try with seed
        json =
            "{\n" +
                "  \"random_sample\" : {\n" +
                "    \"boost\" : 1.0,\n" +
                "    \"probability\" : 0.5,\n" +
                "    \"seed\" : 123\n" +
                "  }\n" +
                "}";
        parsed = (RandomSamplingQueryBuilder) parseQuery(json);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), equalTo(123));

    }
}
