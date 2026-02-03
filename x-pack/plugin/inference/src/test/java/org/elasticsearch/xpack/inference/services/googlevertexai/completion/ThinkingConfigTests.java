/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_BUDGET_FIELD;
import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_CONFIG_FIELD;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ThinkingConfigTests extends AbstractBWCWireSerializationTestCase<ThinkingConfig> {

    public void testNoArgConstructor_createsEmptyConfig() {
        ThinkingConfig thinkingConfig = new ThinkingConfig();
        assertThat(thinkingConfig.isEmpty(), is(true));
        assertThat(thinkingConfig.getThinkingBudget(), is(nullValue()));
    }

    public void testNullValueInConstructor_createsEmptyConfig() {
        Integer nullInt = null;
        ThinkingConfig thinkingConfig = new ThinkingConfig(nullInt);
        assertThat(thinkingConfig.isEmpty(), is(true));
        assertThat(thinkingConfig.getThinkingBudget(), is(nullValue()));
    }

    public void testNonNullValueInConstructor() {
        Integer thinkingBudget = 256;
        ThinkingConfig thinkingConfig = new ThinkingConfig(thinkingBudget);
        assertThat(thinkingConfig.isEmpty(), is(false));
        assertThat(thinkingConfig.getThinkingBudget(), is(thinkingBudget));
    }

    public void testFromMap_withThinkingConfigSpecified_andThinkingBudgetSpecified() {
        ValidationException exception = new ValidationException();
        int thinkingBudget = 256;
        Map<String, Object> settings = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, thinkingBudget)))
        );

        ThinkingConfig result = ThinkingConfig.fromMap(settings, exception);

        assertThat(result, is(new ThinkingConfig(thinkingBudget)));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testFromMap_returnsEmptyThinkingConfig_withThinkingConfigSpecified_andThinkingBudgetNotSpecified() {
        ValidationException exception = new ValidationException();
        Map<String, Object> settings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>()));

        ThinkingConfig result = ThinkingConfig.fromMap(settings, exception);

        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testFromMap_returnsEmptyThinkingConfig_withThinkingConfigNotSpecified_andThinkingBudgetSpecified() {
        ValidationException exception = new ValidationException();
        int thinkingBudget = 256;
        Map<String, Object> settings = new HashMap<>(
            Map.of("not_thinking_config", new HashMap<>(Map.of(THINKING_BUDGET_FIELD, thinkingBudget)))
        );

        ThinkingConfig result = ThinkingConfig.fromMap(settings, exception);

        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testFromMap_returnsEmptyThinkingConfig_withUnknownField() {
        ValidationException exception = new ValidationException();
        int anInt = 42;
        Map<String, Object> settings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of("not_thinking_budget", anInt))));

        ThinkingConfig result = ThinkingConfig.fromMap(settings, exception);

        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testFromMap_returnsEmptyThinkingConfig_addsException_whenFieldIsNotInteger() {
        ValidationException exception = new ValidationException();
        String notAnInt = "not_an_int";
        Map<String, Object> settings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, notAnInt))));

        ThinkingConfig result = ThinkingConfig.fromMap(settings, exception);

        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), hasSize(1));
        assertThat(
            exception.validationErrors().getLast(),
            is("field [thinking_budget] is not of the expected type. The value [" + notAnInt + "] cannot be converted to a [Integer]")
        );
    }

    public void testToXContent() throws IOException {
        ThinkingConfig thinkingConfig = new ThinkingConfig(256);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        thinkingConfig.toXContent(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        String expected = XContentHelper.stripWhitespace("""
            {
              "thinking_config": {
                "thinking_budget": 256
              }
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<ThinkingConfig> instanceReader() {
        return ThinkingConfig::new;
    }

    @Override
    protected ThinkingConfig createTestInstance() {
        return new ThinkingConfig(256);
    }

    @Override
    protected ThinkingConfig mutateInstance(ThinkingConfig instance) throws IOException {
        Integer originalThinkingBudget = instance.getThinkingBudget();
        Integer newThinkingBudget = randomValueOtherThan(originalThinkingBudget, ESTestCase::randomInt);
        return new ThinkingConfig(newThinkingBudget);
    }

    @Override
    protected ThinkingConfig mutateInstanceForVersion(ThinkingConfig instance, TransportVersion version) {
        return instance;
    }
}
