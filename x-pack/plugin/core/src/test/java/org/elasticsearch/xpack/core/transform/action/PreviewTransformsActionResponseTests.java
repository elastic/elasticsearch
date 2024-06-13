/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettingsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PreviewTransformsActionResponseTests extends AbstractXContentSerializingTestCase<Response> {

    private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_preview",
        true,
        args -> {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> docs = (List<Map<String, Object>>) args[0];
            TransformDestIndexSettings generatedDestIndex = (TransformDestIndexSettings) args[1];

            return new Response(docs, generatedDestIndex);
        }
    );

    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> p.mapOrdered(), PreviewTransformAction.Response.PREVIEW);
        PARSER.declareObject(
            optionalConstructorArg(),
            (p, c) -> TransformDestIndexSettings.fromXContent(p),
            PreviewTransformAction.Response.GENERATED_DEST_INDEX_SETTINGS
        );
    }

    public static Response randomPreviewResponse() {
        int size = randomIntBetween(0, 10);
        List<Map<String, Object>> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(Map.of(randomAlphaOfLength(10), Map.of("value1", randomIntBetween(1, 100))));
        }

        return new Response(data, TransformDestIndexSettingsTests.randomDestIndexSettings());
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return randomPreviewResponse();
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

}
