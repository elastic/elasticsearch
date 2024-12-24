/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.migrate.action.CreateIndexFromSourceAction.Request;

import java.io.IOException;
import java.util.Map;

public class CreateFromSourceIndexRequestTests extends AbstractWireSerializingTestCase<Request> {

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
            return new Request(source, dest, randomSettings(), randomMappings());
        }
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {

        String sourceIndex = instance.sourceIndex();
        String destIndex = instance.destIndex();
        Settings settingsOverride = instance.settingsOverride();
        Map<String, Object> mappingsOverride = instance.mappingsOverride();

        switch (between(0, 3)) {
            case 0 -> sourceIndex = randomValueOtherThan(sourceIndex, () -> randomAlphaOfLength(30));
            case 1 -> destIndex = randomValueOtherThan(destIndex, () -> randomAlphaOfLength(30));
            case 2 -> settingsOverride = randomValueOtherThan(settingsOverride, CreateFromSourceIndexRequestTests::randomSettings);
            case 3 -> mappingsOverride = randomValueOtherThan(mappingsOverride, CreateFromSourceIndexRequestTests::randomMappings);
        }
        return new Request(sourceIndex, destIndex, settingsOverride, mappingsOverride);
    }

    public static Map<String, Object> randomMappings() {
        return Map.of("properties", Map.of(randomAlphaOfLength(5), Map.of("type", "keyword")));
    }

    public static Settings randomSettings() {
        return indexSettings(randomIntBetween(1, 10), randomIntBetween(0, 5)).build();
    }
}
