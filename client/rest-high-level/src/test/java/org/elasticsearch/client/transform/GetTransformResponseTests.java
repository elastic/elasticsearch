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

package org.elasticsearch.client.transform;

import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformConfigTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;


public class GetTransformResponseTests extends ESTestCase {

    public void testXContentParser() throws IOException {
        xContentTester(this::createParser,
                GetTransformResponseTests::createTestInstance,
                GetTransformResponseTests::toXContent,
                GetTransformResponse::fromXContent)
                .supportsUnknownFields(false)
                .test();
    }

    private static GetTransformResponse createTestInstance() {
        int numTransforms = randomIntBetween(0, 3);
        List<TransformConfig> transforms = new ArrayList<>();
        for (int i=0; i<numTransforms; i++) {
            transforms.add(TransformConfigTests.randomTransformConfig());
        }
        GetTransformResponse.InvalidTransforms invalidTransforms = null;
        if (randomBoolean()) {
            List<String> invalidIds = Arrays.asList(generateRandomStringArray(5, 6, false, false));
            invalidTransforms = new GetTransformResponse.InvalidTransforms(invalidIds);
        }
        return new GetTransformResponse(transforms, transforms.size() + 10, invalidTransforms);
    }

    private static void toXContent(GetTransformResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field("count", response.getCount());
            builder.field("transforms", response.getTransformConfigurations());
            if (response.getInvalidTransforms() != null) {
                builder.startObject("invalid_transforms");
                builder.field("count", response.getInvalidTransforms().getCount());
                builder.field("transforms", response.getInvalidTransforms().getTransformIds());
                builder.endObject();
            }
        }
        builder.endObject();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }
}
