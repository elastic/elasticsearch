/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class TermsGroupSourceTests extends AbstractSerializingTestCase<TermsGroupSource> {

    public static TermsGroupSource randomTermsGroupSource() {
        return randomTermsGroupSource(Version.CURRENT);
    }

    public static TermsGroupSource randomTermsGroupSource(Version version) {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        ScriptConfig scriptConfig = version.onOrAfter(Version.V_7_7_0)
            ? randomBoolean() ? null : ScriptConfigTests.randomScriptConfig()
            : null;
        boolean missingBucket = version.onOrAfter(Version.V_7_10_0) ? randomBoolean() : false;
        return new TermsGroupSource(field, scriptConfig, missingBucket);
    }

    public static TermsGroupSource randomTermsGroupSourceNoScript() {
        String field = randomAlphaOfLengthBetween(1, 20);
        return new TermsGroupSource(field, null, randomBoolean());
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
    protected Reader<TermsGroupSource> instanceReader() {
        return TermsGroupSource::new;
    }

}
