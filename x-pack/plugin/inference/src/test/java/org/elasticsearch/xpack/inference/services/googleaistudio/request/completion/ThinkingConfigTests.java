/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_BUDGET_FIELD;
import static org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig.THINKING_CONFIG_FIELD;
import static org.hamcrest.Matchers.empty;
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

    public void testOf_withThinkingConfigSpecified_andThinkingBudgetSpecified() {
        ValidationException exception = new ValidationException();
        int thinkingBudget = 256;
        Map<String, Object> settings = new HashMap<>(
            Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of(THINKING_BUDGET_FIELD, thinkingBudget)))
        );

        ThinkingConfig result = ThinkingConfig.of(settings, exception, "test", ConfigurationParseContext.REQUEST);

        assertThat(result, is(new ThinkingConfig(thinkingBudget)));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testOf_returnsEmptyThinkingConfig_withThinkingConfigSpecified_andThinkingBudgetNotSpecified() {
        ValidationException exception = new ValidationException();
        Map<String, Object> settings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>()));

        ThinkingConfig result = ThinkingConfig.of(settings, exception, "test", ConfigurationParseContext.REQUEST);

        assertThat(result.getThinkingBudget(), is(nullValue()));
        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testOf_returnsEmptyThinkingConfig_withThinkingConfigNotSpecified_andThinkingBudgetSpecified() {
        ValidationException exception = new ValidationException();
        int thinkingBudget = 256;
        Map<String, Object> settings = new HashMap<>(
            Map.of("not_thinking_config", new HashMap<>(Map.of(THINKING_BUDGET_FIELD, thinkingBudget)))
        );

        ThinkingConfig result = ThinkingConfig.of(settings, exception, "test", ConfigurationParseContext.REQUEST);

        assertThat(result.getThinkingBudget(), is(nullValue()));
        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), is(empty()));
    }

    public void testOf_throwsException_withUnknownField_andRequestContext() {
        ValidationException exception = new ValidationException();
        int anInt = 42;
        Map<String, Object> settings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of("not_thinking_budget", anInt))));

        Exception thrownException = expectThrows(
            ElasticsearchStatusException.class,
            () -> ThinkingConfig.of(settings, exception, "test", ConfigurationParseContext.REQUEST)
        );

        assertThat(
            thrownException.getMessage(),
            is("Configuration contains settings [{not_thinking_budget=42}] unknown to the [test] service")
        );
    }

    public void testOf_returnsEmptyThinkingConfig_withUnknownField_andPersistentContext() {
        ValidationException exception = new ValidationException();
        int anInt = 42;
        Map<String, Object> settings = new HashMap<>(Map.of(THINKING_CONFIG_FIELD, new HashMap<>(Map.of("not_thinking_budget", anInt))));

        ThinkingConfig result = ThinkingConfig.of(settings, exception, "test", ConfigurationParseContext.PERSISTENT);

        assertThat(result.getThinkingBudget(), is(nullValue()));
        assertThat(result.isEmpty(), is(true));
        assertThat(exception.validationErrors(), is(empty()));
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
        return randomValueOtherThan(instance, () -> new ThinkingConfig(randomInt()));
    }

    @Override
    protected ThinkingConfig mutateInstanceForVersion(ThinkingConfig instance, TransportVersion version) {
        return instance;
    }
}
