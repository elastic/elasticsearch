/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.migrate.action.CreateIndexFromSourceAction.Request;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class CreateFromSourceIndexRequestTests extends AbstractWireSerializingTestCase<Request> {

    public void testToAndFromXContent() throws IOException {
        var request = createTestInstance();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(request, xContentType, EMPTY_PARAMS, humanReadable);

        var parsedRequest = new Request(
            randomValueOtherThan(request.sourceIndex(), () -> randomAlphanumericOfLength(30)),
            randomValueOtherThan(request.sourceIndex(), () -> randomAlphanumericOfLength(30))
        );
        try (XContentParser xParser = createParser(xContentType.xContent(), originalBytes)) {
            parsedRequest.fromXContent(xParser);
        }

        // source and dest won't be equal
        assertNotEquals(request, parsedRequest);
        assertNotEquals(request.sourceIndex(), parsedRequest.sourceIndex());
        assertNotEquals(request.destIndex(), parsedRequest.destIndex());

        // but fields in xcontent will be equal
        assertEquals(request.settingsOverride(), parsedRequest.settingsOverride());
        assertEquals(request.mappingsOverride(), parsedRequest.mappingsOverride());

        BytesReference finalBytes = toShuffledXContent(parsedRequest, xContentType, EMPTY_PARAMS, humanReadable);
        ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        String source = randomAlphaOfLength(30);
        String dest = randomAlphaOfLength(30);
        if (randomBoolean()) {
            return new Request(source, dest);
        } else {
            return new Request(source, dest, randomSettings(), randomMappings(), randomBoolean());
        }
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {

        String sourceIndex = instance.sourceIndex();
        String destIndex = instance.destIndex();
        Settings settingsOverride = instance.settingsOverride();
        Map<String, Object> mappingsOverride = instance.mappingsOverride();
        boolean removeIndexBlocks = instance.removeIndexBlocks();

        switch (between(0, 4)) {
            case 0 -> sourceIndex = randomValueOtherThan(sourceIndex, () -> randomAlphaOfLength(30));
            case 1 -> destIndex = randomValueOtherThan(destIndex, () -> randomAlphaOfLength(30));
            case 2 -> settingsOverride = randomValueOtherThan(settingsOverride, CreateFromSourceIndexRequestTests::randomSettings);
            case 3 -> mappingsOverride = randomValueOtherThan(mappingsOverride, CreateFromSourceIndexRequestTests::randomMappings);
            case 4 -> removeIndexBlocks = removeIndexBlocks == false;
        }
        return new Request(sourceIndex, destIndex, settingsOverride, mappingsOverride, removeIndexBlocks);
    }

    public static Map<String, Object> randomMappings() {
        var randMappings = Map.of("properties", Map.of(randomAlphaOfLength(5), Map.of("type", "keyword")));
        return randomBoolean() ? Map.of() : Map.of("_doc", randMappings);
    }

    public static Settings randomSettings() {
        return randomBoolean() ? Settings.EMPTY : indexSettings(randomIntBetween(1, 10), randomIntBetween(0, 5)).build();
    }
}
