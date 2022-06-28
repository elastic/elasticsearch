/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.function.Predicate;

public class UpdateSettingsRequestTests extends AbstractXContentTestCase<UpdateSettingsRequest> {

    /** True if the setting should be enclosed in a settings map. */
    private final boolean enclosedSettings;
    /** True if the request should contain unknown top-level properties. */
    private final boolean unknownFields;

    public UpdateSettingsRequestTests() {
        this(randomBoolean(), randomBoolean());
    }

    private UpdateSettingsRequestTests(boolean enclosedSettings, boolean unknownFields) {
        this.enclosedSettings = enclosedSettings;
        this.unknownFields = unknownFields;
    }

    final UpdateSettingsRequestTests withEnclosedSettings() {
        return new UpdateSettingsRequestTests(true, unknownFields);
    }

    final UpdateSettingsRequestTests withoutEnclosedSettings() {
        return new UpdateSettingsRequestTests(false, unknownFields);
    }

    final UpdateSettingsRequestTests withUnknownFields() {
        return new UpdateSettingsRequestTests(enclosedSettings, true);
    }

    final UpdateSettingsRequestTests withoutUnknownFields() {
        return new UpdateSettingsRequestTests(enclosedSettings, false);
    }

    @Override
    protected UpdateSettingsRequest createTestInstance() {
        return createTestInstance(enclosedSettings);
    }

    private static UpdateSettingsRequest createTestInstance(boolean enclosedSettings) {
        UpdateSettingsRequest testRequest = UpdateSettingsRequestSerializationTests.createTestItem();
        if (enclosedSettings) {
            UpdateSettingsRequest requestWithEnclosingSettings = new UpdateSettingsRequest(testRequest.settings()) {
                public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                    builder.startObject();
                    builder.startObject("settings");
                    this.settings().toXContent(builder, params);
                    builder.endObject();
                    builder.endObject();
                    return builder;
                }
            };
            return requestWithEnclosingSettings;
        }
        return testRequest;
    }

    @Override
    protected UpdateSettingsRequest doParseInstance(XContentParser parser) throws IOException {
        return new UpdateSettingsRequest().fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        // if the settings are enclosed as a "settings" object
        // then all other top-level elements will be ignored during the parsing
        return enclosedSettings && unknownFields;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        if (enclosedSettings) {
            return field -> field.startsWith("settings");
        }
        return field -> true;
    }

    @Override
    protected void assertEqualInstances(UpdateSettingsRequest expectedInstance, UpdateSettingsRequest newInstance) {
        // here only the settings should be tested, as this test covers explicitly only the XContent parsing
        // the rest of the request fields are tested by the SerializingTests
        super.assertEqualInstances(
            new UpdateSettingsRequest(expectedInstance.settings()),
            new UpdateSettingsRequest(newInstance.settings())
        );
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        // if enclosedSettings are used, disable the XContentEquivalence check as the
        // parsed.toXContent is not equivalent to the test instance
        return enclosedSettings == false;
    }

    public void testWithEnclosedSettingsWithUnknownFields() throws IOException {
        testFromXContent((new UpdateSettingsRequestTests()).withEnclosedSettings().withUnknownFields());
    }

    public void testWithEnclosedSettingsWithoutUnknownFields() throws IOException {
        testFromXContent((new UpdateSettingsRequestTests()).withEnclosedSettings().withoutUnknownFields());
    }

    public void testWithoutEnclosedSettingsWithoutUnknownFields() throws IOException {
        testFromXContent((new UpdateSettingsRequestTests()).withoutEnclosedSettings().withoutUnknownFields());
    }

    private static void testFromXContent(UpdateSettingsRequestTests test) throws IOException {
        AbstractXContentTestCase.testFromXContent(
            NUMBER_OF_TEST_RUNS / 2,
            test::createTestInstance,
            test.supportsUnknownFields(),
            test.getShuffleFieldsExceptions(),
            test.getRandomFieldsExcludeFilter(),
            test::createParser,
            test::doParseInstance,
            test::assertEqualInstances,
            test.assertToXContentEquivalence(),
            test.getToXContentParams()
        );
    }

    /** Tests that mixed requests, containing both an enclosed settings and top-level fields, generate a log warning message. */
    public void testLogIgnoredFields() throws Exception {
        final Logger settingsLogger = LogManager.getLogger(UpdateSettingsRequest.class);
        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(settingsLogger, appender);
        appender.start();
        appender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "Ignored settings warning",
                settingsLogger.getName(),
                Level.WARN,
                "Ignoring top-level request settings"
            )
        );
        try {
            UpdateSettingsRequestTests test = (new UpdateSettingsRequestTests()).withEnclosedSettings().withUnknownFields();
            UpdateSettingsRequest updateSettingsRequest = test.createTestInstance();
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference originalXContent = XContentHelper.toXContent(
                updateSettingsRequest,
                xContentType,
                ToXContent.EMPTY_PARAMS,
                false
            );
            BytesReference updatedXContent = XContentTestUtils.insertRandomFields(
                xContentType,
                originalXContent,
                test.getRandomFieldsExcludeFilter(),
                random()
            );
            XContentParser parser = test.createParser(XContentFactory.xContent(xContentType), updatedXContent);
            UpdateSettingsRequest parsedRequest = (new UpdateSettingsRequest()).fromXContent(parser);
            test.assertEqualInstances(updateSettingsRequest, parsedRequest);
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.removeAppender(settingsLogger, appender);
        }
    }
}
