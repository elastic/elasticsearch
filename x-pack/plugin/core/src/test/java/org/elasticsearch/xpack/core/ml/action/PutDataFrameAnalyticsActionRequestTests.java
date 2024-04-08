/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction.Request;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetectionTests;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutDataFrameAnalyticsActionRequestTests extends AbstractXContentSerializingTestCase<Request> {

    private String id;

    @Before
    public void setUpId() {
        id = DataFrameAnalyticsConfigTests.randomValidId();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected Request createTestInstance() {
        return new Request(DataFrameAnalyticsConfigTests.createRandom(id));
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(id, parser);
    }

    public void testValidate_GivenRequestWithIncludedAnalyzedFieldThatIsExcludedInSourceFiltering() {
        DataFrameAnalyticsSource source = new DataFrameAnalyticsSource(
            new String[] { "index" },
            null,
            FetchSourceContext.of(true, null, new String[] { "excluded" }),
            null
        );
        FetchSourceContext analyzedFields = FetchSourceContext.of(true, new String[] { "excluded" }, null);
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId("foo")
            .setSource(source)
            .setAnalysis(OutlierDetectionTests.createRandom())
            .setAnalyzedFields(analyzedFields)
            .buildForExplain();
        Request request = new Request(config);

        Exception e = request.validate();

        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), containsString("field [excluded] is included in [analyzed_fields] but not in [source._source]"));
    }

    public void testValidate_GivenRequestWithIncludedAnalyzedFieldThatIsIncludedInSourceFiltering() {
        DataFrameAnalyticsSource source = new DataFrameAnalyticsSource(
            new String[] { "index" },
            null,
            FetchSourceContext.of(true, new String[] { "included" }, null),
            null
        );
        FetchSourceContext analyzedFields = FetchSourceContext.of(true, new String[] { "included" }, null);
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId("foo")
            .setSource(source)
            .setAnalysis(OutlierDetectionTests.createRandom())
            .setAnalyzedFields(analyzedFields)
            .buildForExplain();
        Request request = new Request(config);

        Exception e = request.validate();

        assertThat(e, is(nullValue()));
    }

    public void testDefaultTimeout() {
        assertThat(createTestInstance().timeout(), is(notNullValue()));
    }
}
