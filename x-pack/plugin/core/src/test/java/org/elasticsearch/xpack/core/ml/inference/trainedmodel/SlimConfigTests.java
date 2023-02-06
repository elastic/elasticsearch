/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;

public class SlimConfigTests extends InferenceConfigItemTestCase<SlimConfig> {

    public static SlimConfig createRandom() {
        // create a tokenization config with a no span setting.
        var tokenization = new BertTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.FIRST),
            null
        );

        return new SlimConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean() ? null : tokenization,
            randomBoolean() ? null : randomAlphaOfLength(5)
        );
    }

    @Override
    protected Writeable.Reader<SlimConfig> instanceReader() {
        return SlimConfig::new;
    }

    @Override
    protected SlimConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected SlimConfig mutateInstance(SlimConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected SlimConfig doParseInstance(XContentParser parser) throws IOException {
        return SlimConfig.fromXContentLenient(parser);
    }

    @Override
    protected SlimConfig mutateInstanceForVersion(SlimConfig instance, Version version) {
        return instance;
    }

    public void testBertTokenizationOnly() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new SlimConfig(null, RobertaTokenizationTests.createRandom(), null)
        );
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("SLIM must be configured with BERT tokenizer, [roberta] given", e.getMessage());
    }
}
