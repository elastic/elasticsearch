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

package org.elasticsearch.client.dataframe;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigTests;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfigTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.client.dataframe.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.containsString;

public class PreviewDataFrameTransformRequestTests extends AbstractXContentTestCase<PreviewDataFrameTransformRequest> {
    @Override
    protected PreviewDataFrameTransformRequest createTestInstance() {
        return new PreviewDataFrameTransformRequest(DataFrameTransformConfigTests.randomDataFrameTransformConfig());
    }

    @Override
    protected PreviewDataFrameTransformRequest doParseInstance(XContentParser parser) throws IOException {
        return new PreviewDataFrameTransformRequest(DataFrameTransformConfig.fromXContent(parser));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new DataFrameNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }

    public void testValidate() {
        assertFalse(new PreviewDataFrameTransformRequest(DataFrameTransformConfigTests.randomDataFrameTransformConfig())
                .validate().isPresent());
        assertThat(new PreviewDataFrameTransformRequest(null).validate().get().getMessage(),
                containsString("preview requires a non-null data frame config"));

        // null id and destination is valid
        DataFrameTransformConfig config = DataFrameTransformConfig.forPreview(randomSourceConfig(), PivotConfigTests.randomPivotConfig());

        assertFalse(new PreviewDataFrameTransformRequest(config).validate().isPresent());

        // null source is not valid
        config = DataFrameTransformConfig.builder().setPivotConfig(PivotConfigTests.randomPivotConfig()).build();

        Optional<ValidationException> error = new PreviewDataFrameTransformRequest(config).validate();
        assertTrue(error.isPresent());
        assertThat(error.get().getMessage(), containsString("data frame transform source cannot be null"));
    }
}
