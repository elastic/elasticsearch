/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TermsGroupSourceTests extends AbstractXContentSerializingTestCase<TermsGroupSource> {

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
        return new TermsGroupSource(field, scriptConfig, missingBucket);
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
}
