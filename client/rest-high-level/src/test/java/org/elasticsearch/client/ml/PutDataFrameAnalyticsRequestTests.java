/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.client.ml.dataframe.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class PutDataFrameAnalyticsRequestTests extends AbstractXContentTestCase<PutDataFrameAnalyticsRequest> {

    public void testValidate_Ok() {
        assertFalse(createTestInstance().validate().isPresent());
    }

    public void testValidate_Failure() {
        Optional<ValidationException> exception = new PutDataFrameAnalyticsRequest(null).validate();
        assertTrue(exception.isPresent());
        assertThat(exception.get().getMessage(), containsString("put requires a non-null data frame analytics config"));
    }

    @Override
    protected PutDataFrameAnalyticsRequest createTestInstance() {
        return new PutDataFrameAnalyticsRequest(DataFrameAnalyticsConfigTests.randomDataFrameAnalyticsConfig());
    }

    @Override
    protected PutDataFrameAnalyticsRequest doParseInstance(XContentParser parser) throws IOException {
        return new PutDataFrameAnalyticsRequest(DataFrameAnalyticsConfig.fromXContent(parser));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }
}
