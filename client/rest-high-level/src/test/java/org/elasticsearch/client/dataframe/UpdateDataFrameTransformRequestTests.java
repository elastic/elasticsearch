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
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdate;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdateTests.randomDataFrameTransformConfigUpdate;
import static org.hamcrest.Matchers.containsString;

public class UpdateDataFrameTransformRequestTests extends AbstractXContentTestCase<UpdateDataFrameTransformRequest> {

    public void testValidate() {
        assertFalse(createTestInstance().validate().isPresent());

        DataFrameTransformConfigUpdate config = randomDataFrameTransformConfigUpdate();

        Optional<ValidationException> error = new UpdateDataFrameTransformRequest(config, null).validate();
        assertTrue(error.isPresent());
        assertThat(error.get().getMessage(), containsString("data frame transform id cannot be null"));

        error = new UpdateDataFrameTransformRequest(null, "123").validate();
        assertTrue(error.isPresent());
        assertThat(error.get().getMessage(), containsString("put requires a non-null data frame config"));
    }

    private final String transformId = randomAlphaOfLength(10);
    @Override
    protected UpdateDataFrameTransformRequest createTestInstance() {
        return new UpdateDataFrameTransformRequest(randomDataFrameTransformConfigUpdate(), transformId);
    }

    @Override
    protected UpdateDataFrameTransformRequest doParseInstance(XContentParser parser) throws IOException {
        return new UpdateDataFrameTransformRequest(DataFrameTransformConfigUpdate.fromXContent(parser), transformId);
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
}
