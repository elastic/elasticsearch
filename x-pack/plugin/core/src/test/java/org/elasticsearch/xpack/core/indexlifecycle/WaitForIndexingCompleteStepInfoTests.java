/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.WaitForIndexingCompleteStep.Info;

import java.io.IOException;

import static org.elasticsearch.index.RandomCreateIndexGenerator.randomIndexSettings;

public class WaitForIndexingCompleteStepInfoTests extends AbstractXContentTestCase<Info> {

    private static final ConstructingObjectParser<Info, Void> PARSER =
        new ConstructingObjectParser<>("wait_for_indexing_complete_info",
        args -> new Info((Settings) args[0]));
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(),
            (p, c) -> Settings.fromXContent(p), Info.INDEX_SETTINGS_FIELD);
        PARSER.declareString((i, s) -> {}, Info.MESSAGE_FIELD);
    }

    @Override
    protected Info createTestInstance() {
        return new Info(randomIndexSettings());
    }

    @Override
    protected Info doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public final void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copyInstance, this::mutateInstance);
        }
    }

    protected final Info copyInstance(Info instance) throws IOException {
        return new Info(instance.getIndexSettings());
    }

    protected Info mutateInstance(Info instance) throws IOException {
        Settings.Builder newSettings = Settings.builder().put(instance.getIndexSettings());
        newSettings.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
        return new Info(newSettings.build());
    }

}
