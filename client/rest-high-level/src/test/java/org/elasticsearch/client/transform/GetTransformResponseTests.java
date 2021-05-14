/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
