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
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.QueryProvider;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MlDeprecationChecksTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testCheckDataFeedQuery() throws IOException {
        DatafeedConfig.Builder goodDatafeed = new DatafeedConfig.Builder("good-df", "job-id");
        goodDatafeed.setIndices(Collections.singletonList("some-index"));
        goodDatafeed.setParsedQuery(QueryBuilders.termQuery("foo", "bar"));
        assertNull(MlDeprecationChecks.checkDataFeedQuery(goodDatafeed.build(), xContentRegistry()));

        DatafeedConfig.Builder deprecatedDatafeed = new DatafeedConfig.Builder("df-with-deprecated-query", "job-id");
        deprecatedDatafeed.setIndices(Collections.singletonList("some-index"));
        Map<String, Object> qs = new HashMap<>();
        qs.put("query", "foo");
        qs.put("use_dis_max", true);
        Map<String, Object> query = Collections.singletonMap("query_string", qs);
        deprecatedDatafeed.setQueryProvider(new QueryProvider(query,
            XContentObjectTransformer.queryBuilderTransformer(xContentRegistry()).fromMap(query),
            null));
        
        DeprecationIssue issue = MlDeprecationChecks.checkDataFeedQuery(deprecatedDatafeed.build(), xContentRegistry());
        assertNotNull(issue);
        assertThat(issue.getDetails(), equalTo("[Deprecated field [use_dis_max] used, replaced by [Set [tie_breaker] to 1 instead]]"));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.WARNING));
        assertThat(issue.getMessage(), equalTo("Datafeed [df-with-deprecated-query] uses deprecated query options"));
        assertThat(issue.getUrl(), equalTo("https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                "#breaking_70_search_changes"));
    }
}
