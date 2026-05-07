/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TermsGroupSourceTests extends AbstractSerializingTransformTestCase<TermsGroupSource> {

    public static TermsGroupSource randomTermsGroupSource() {
        return randomTermsGroupSource(TransformConfigVersion.CURRENT);
    }

    public static TermsGroupSource randomTermsGroupSourceNoScript() {
        return randomTermsGroupSource(TransformConfigVersion.CURRENT, false);
    }

    public static TermsGroupSource randomTermsGroupSourceNoScript(String fieldPrefix) {
        return randomTermsGroupSource(TransformConfigVersion.CURRENT, false, fieldPrefix);
    }

    public static TermsGroupSource randomTermsGroupSource(TransformConfigVersion version) {
        return randomTermsGroupSource(TransformConfigVersion.CURRENT, randomBoolean());
    }

    public static TermsGroupSource randomTermsGroupSource(TransformConfigVersion version, boolean withScript) {
        return randomTermsGroupSource(TransformConfigVersion.CURRENT, withScript, "");
    }

    public static TermsGroupSource randomTermsGroupSource(TransformConfigVersion version, boolean withScript, String fieldPrefix) {
        ScriptConfig scriptConfig = null;
        String field;

        // either a field or a script must be specified, it's possible to have both, but disallowed to have none
        if (version.onOrAfter(TransformConfigVersion.V_7_7_0) && withScript) {
            scriptConfig = ScriptConfigTests.randomScriptConfig();
            field = randomBoolean() ? null : fieldPrefix + randomAlphaOfLengthBetween(1, 20);
        } else {
            field = fieldPrefix + randomAlphaOfLengthBetween(1, 20);
        }

        boolean missingBucket = version.onOrAfter(TransformConfigVersion.V_7_10_0) ? randomBoolean() : false;
        Integer maxTermsForChangeDetection = randomBoolean() ? null : randomFrom(-1, 0, randomIntBetween(1, 100_000));
        return new TermsGroupSource(field, scriptConfig, missingBucket, maxTermsForChangeDetection);
    }

    @Override
    protected TermsGroupSource doParseInstance(XContentParser parser) throws IOException {
        return TermsGroupSource.fromXContent(parser, false);
    }

    @Override
    protected TermsGroupSource createTestInstance() {
        return randomTermsGroupSource();
    }

    @Override
    protected TermsGroupSource mutateInstance(TermsGroupSource instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TermsGroupSource mutateInstanceForVersion(TermsGroupSource instance, TransportVersion version) {
        if (version.supports(TermsGroupSource.MAX_TERMS_FOR_CHANGE_DETECTION) == false) {
            return new TermsGroupSource(instance.getField(), instance.getScriptConfig(), instance.getMissingBucket(), null);
        }
        return instance;
    }

    @Override
    protected Reader<TermsGroupSource> instanceReader() {
        return TermsGroupSource::new;
    }

    public void testFailOnFieldAndScriptBothBeingNull() throws IOException {
        String json = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            TermsGroupSource group = TermsGroupSource.fromXContent(parser, true);
            assertThat(group.getField(), is(nullValue()));
            assertThat(group.getScriptConfig(), is(nullValue()));
            ValidationException validationException = group.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("Required one of fields [field, script], but none were specified"));
        }
    }

    public void testValidateMaxTermsForChangeDetection() {
        TermsGroupSource group = new TermsGroupSource("field", null, false, null);
        assertThat(group.validate(null), is(nullValue()));

        group = new TermsGroupSource("field", null, false, 0);
        assertThat(group.validate(null), is(nullValue()));

        group = new TermsGroupSource("field", null, false, -1);
        assertThat(group.validate(null), is(nullValue()));

        group = new TermsGroupSource("field", null, false, 1);
        assertThat(group.validate(null), is(nullValue()));

        group = new TermsGroupSource("field", null, false, 10_000);
        assertThat(group.validate(null), is(nullValue()));

        group = new TermsGroupSource("field", null, false, -3);
        ValidationException validationException = group.validate(null);
        assertThat(validationException, is(notNullValue()));
        assertThat(validationException.getMessage(), containsString("max_terms_for_change_detection [-3] is out of range"));

        group = new TermsGroupSource("field", null, false, -2);
        validationException = group.validate(null);
        assertThat(validationException, is(notNullValue()));
        assertThat(validationException.getMessage(), containsString("max_terms_for_change_detection [-2] is out of range"));
    }

    public void testMaxTermsForChangeDetectionParsing() throws IOException {
        String json = "{\"field\": \"my_field\", \"max_terms_for_change_detection\": 5000}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            TermsGroupSource group = TermsGroupSource.fromXContent(parser, false);
            assertThat(group.getField(), is("my_field"));
            assertThat(group.getMaxTermsForChangeDetection(), is(5000));
        }

        json = "{\"field\": \"my_field\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            TermsGroupSource group = TermsGroupSource.fromXContent(parser, false);
            assertThat(group.getField(), is("my_field"));
            assertThat(group.getMaxTermsForChangeDetection(), is(nullValue()));
        }
    }

    /**
     * Returns a copy of the given {@link GroupConfig} with {@code maxTermsForChangeDetection} stripped
     * from any {@link TermsGroupSource} entries. The original raw source map is preserved so that
     * the result matches what stream deserialization produces at older versions.
     */
    public static GroupConfig mutateGroupConfigForBwc(GroupConfig groupConfig) {
        Map<String, SingleGroupSource> mutated = new LinkedHashMap<>();
        for (Map.Entry<String, SingleGroupSource> entry : groupConfig.getGroups().entrySet()) {
            SingleGroupSource source = entry.getValue();
            if (source instanceof TermsGroupSource tgs) {
                source = new TermsGroupSource(tgs.getField(), tgs.getScriptConfig(), tgs.getMissingBucket(), null);
            }
            mutated.put(entry.getKey(), source);
        }
        return new GroupConfig(groupConfig.getSource(), mutated);
    }
}
