/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class TermsGroupSourceTests extends AbstractSerializingTestCase<TermsGroupSource> {

    public static TermsGroupSource randomTermsGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);

        return new TermsGroupSource(field);
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
