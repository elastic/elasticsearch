/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.pivot.ScriptConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TermsGroupSourceTests extends AbstractResponseTestCase<
    TermsGroupSource,
    org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource> {

    public static TermsGroupSource randomTermsGroupSource() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        ScriptConfig scriptConfig = randomBoolean() ? null : DateHistogramGroupSourceTests.randomScriptConfig();
        boolean missingBucket = randomBoolean();
        return new TermsGroupSource(field, scriptConfig, missingBucket);
    }

    @Override
    protected TermsGroupSource createServerTestInstance(XContentType xContentType) {
        return randomTermsGroupSource();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource doParseToClientInstance(XContentParser parser)
        throws IOException {
        return org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        TermsGroupSource serverTestInstance,
        org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource clientInstance
    ) {
        assertThat(serverTestInstance.getField(), equalTo(clientInstance.getField()));
        if (serverTestInstance.getScriptConfig() != null) {
            assertThat(serverTestInstance.getScriptConfig().getScript(), equalTo(clientInstance.getScript()));
        } else {
            assertNull(clientInstance.getScript());
        }
    }

}
