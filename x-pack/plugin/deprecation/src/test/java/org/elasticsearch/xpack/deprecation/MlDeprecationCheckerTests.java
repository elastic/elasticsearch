/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

public class MlDeprecationCheckerTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testEnabled() {
        MlDeprecationChecker mlDeprecationChecker = new MlDeprecationChecker();
        assertThat(mlDeprecationChecker.enabled(Settings.EMPTY), is(true));
        assertThat(
            mlDeprecationChecker.enabled(
                Settings.builder().put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), Boolean.toString(false)).build()
            ),
            is(false)
        );
    }

    public void testCheckDataFeedQuery() {
        DatafeedConfig.Builder goodDatafeed = new DatafeedConfig.Builder("good-df", "job-id");
        goodDatafeed.setIndices(Collections.singletonList("some-index"));
        goodDatafeed.setParsedQuery(QueryBuilders.termQuery("foo", "bar"));
        assertThat(MlDeprecationChecker.checkDataFeedQuery(goodDatafeed.build(), xContentRegistry()).isPresent(), is(false));

        DatafeedConfig.Builder deprecatedDatafeed = new DatafeedConfig.Builder("df-with-deprecated-query", "job-id");
        deprecatedDatafeed.setIndices(Collections.singletonList("some-index"));
        // TODO: once some query syntax has been removed from 8.0 and deprecated in 7.x reinstate this test
        // to check that particular query syntax causes a deprecation warning
        /*
        Map<String, Object> qs = new HashMap<>();
        qs.put("query", "foo");
        qs.put("use_dis_max", true);
        Map<String, Object> query = Collections.singletonMap("query_string", qs);
        deprecatedDatafeed.setQuery(query);

        DeprecationIssue issue = MlDeprecationChecks.checkDataFeedQuery(deprecatedDatafeed.build());
        assertNotNull(issue);
        assertThat(issue.getDetails(), equalTo("[Deprecated field [use_dis_max] used, replaced by [Set [tie_breaker] to 1 instead]]"));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.WARNING));
        assertThat(issue.getMessage(), equalTo("Datafeed [df-with-deprecated-query] uses deprecated query options"));
        assertThat(issue.getUrl(), equalTo("https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html" +
                "#breaking_80_search_changes"));
        */
    }
}
