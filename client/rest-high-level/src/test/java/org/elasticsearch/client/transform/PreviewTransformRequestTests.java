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
import org.elasticsearch.client.transform.transforms.latest.LatestConfigTests;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.client.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.containsString;

public class PreviewTransformRequestTests extends AbstractXContentTestCase<PreviewTransformRequest> {
    @Override
    protected PreviewTransformRequest createTestInstance() {
        return randomBoolean()
            ? new PreviewTransformRequest(randomAlphaOfLengthBetween(1, 10))
            : new PreviewTransformRequest(TransformConfigTests.randomTransformConfig());
    }

    @Override
    protected PreviewTransformRequest doParseInstance(XContentParser parser) throws IOException {
        return fromXContent(parser);
    }

    public static PreviewTransformRequest fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> content = parser.map();
        if (content.size() == 1 && content.containsKey(TransformConfig.ID.getPreferredName())) {
            // The request only contains transform id so instead of parsing TransformConfig (which is pointless in this case),
            // let's just fetch the transform config by id later on.
            String transformId = (String) content.get(TransformConfig.ID.getPreferredName());
            return new PreviewTransformRequest(transformId);
        }
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(content);
            XContentParser newParser = XContentType.JSON.xContent()
                .createParser(
                    parser.getXContentRegistry(),
                    LoggingDeprecationHandler.INSTANCE,
                    BytesReference.bytes(xContentBuilder).streamInput()
                )
        ) {
            return new PreviewTransformRequest(TransformConfig.fromXContent(newParser));
        }
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

    public void testConstructorThrowsNPE() {
        expectThrows(NullPointerException.class, () -> new PreviewTransformRequest((String) null));
        expectThrows(NullPointerException.class, () -> new PreviewTransformRequest((TransformConfig) null));
    }

    public void testValidate() {
        assertThat(new PreviewTransformRequest(TransformConfigTests.randomTransformConfig()).validate(), isEmpty());

        // null id and destination is valid
        TransformConfig config = TransformConfig.forPreview(randomSourceConfig(), PivotConfigTests.randomPivotConfig());

        assertThat(new PreviewTransformRequest(config).validate(), isEmpty());

        // null source is not valid
        config = TransformConfig.builder().setPivotConfig(PivotConfigTests.randomPivotConfig()).build();

        Optional<ValidationException> error = new PreviewTransformRequest(config).validate();
        assertThat(error, isPresent());
        assertThat(error.get().getMessage(), containsString("transform source cannot be null"));

        // null id and destination is valid
        config = TransformConfig.forPreview(randomSourceConfig(), LatestConfigTests.randomLatestConfig());

        assertThat(new PreviewTransformRequest(config).validate(), isEmpty());
    }
}
