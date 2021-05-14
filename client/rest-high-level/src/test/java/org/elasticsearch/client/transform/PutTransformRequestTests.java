/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformConfigTests;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;

public class PutTransformRequestTests extends AbstractXContentTestCase<PutTransformRequest> {

    public void testValidate() {
        assertFalse(createTestInstance().validate().isPresent());

        TransformConfig config = TransformConfig.builder().setPivotConfig(PivotConfigTests.randomPivotConfig()).build();

        Optional<ValidationException> error = new PutTransformRequest(config).validate();
        assertTrue(error.isPresent());
        assertThat(error.get().getMessage(), containsString("transform id cannot be null"));
        assertThat(error.get().getMessage(), containsString("transform source cannot be null"));
        assertThat(error.get().getMessage(), containsString("transform destination cannot be null"));

        error = new PutTransformRequest(null).validate();
        assertTrue(error.isPresent());
        assertThat(error.get().getMessage(), containsString("put requires a non-null transform config"));
    }

    @Override
    protected PutTransformRequest createTestInstance() {
        return new PutTransformRequest(TransformConfigTests.randomTransformConfig());
    }

    @Override
    protected PutTransformRequest doParseInstance(XContentParser parser) throws IOException {
        return new PutTransformRequest(TransformConfig.fromXContent(parser));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        return new NamedXContentRegistry(namedXContents);
    }
}
