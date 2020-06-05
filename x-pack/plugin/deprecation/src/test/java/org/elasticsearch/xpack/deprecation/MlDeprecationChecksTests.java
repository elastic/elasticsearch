/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.Collections;

public class MlDeprecationChecksTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testCheckDataFeedQuery() {
        DatafeedConfig.Builder goodDatafeed = new DatafeedConfig.Builder("good-df", "job-id");
        goodDatafeed.setIndices(Collections.singletonList("some-index"));
        goodDatafeed.setParsedQuery(QueryBuilders.termQuery("foo", "bar"));
        assertNull(MlDeprecationChecks.checkDataFeedQuery(goodDatafeed.build(), xContentRegistry()));

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
