/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TextExpansionConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<TextExpansionConfigUpdate> {

    public static TextExpansionConfigUpdate randomUpdate() {
        TextExpansionConfigUpdate.Builder builder = new TextExpansionConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    public static TextExpansionConfigUpdate mutateForVersion(TextExpansionConfigUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_1_0)) {
            return new TextExpansionConfigUpdate(instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    protected Writeable.Reader<TextExpansionConfigUpdate> instanceReader() {
        return TextExpansionConfigUpdate::new;
    }

    @Override
    protected TextExpansionConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected TextExpansionConfigUpdate mutateInstance(TextExpansionConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected TextExpansionConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TextExpansionConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected TextExpansionConfigUpdate mutateInstanceForVersion(TextExpansionConfigUpdate instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    @Override
    Tuple<Map<String, Object>, TextExpansionConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        TextExpansionConfigUpdate expected = new TextExpansionConfigUpdate("ml-results", expectedTokenization);
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    TextExpansionConfigUpdate fromMap(Map<String, Object> map) {
        return TextExpansionConfigUpdate.fromMap(map);
    }
}
