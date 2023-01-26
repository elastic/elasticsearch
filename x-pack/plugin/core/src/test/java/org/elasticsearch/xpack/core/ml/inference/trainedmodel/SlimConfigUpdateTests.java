/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SlimConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<SlimConfigUpdate> {

    public static SlimConfigUpdate randomUpdate() {
        SlimConfigUpdate.Builder builder = new SlimConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    public static SlimConfigUpdate mutateForVersion(SlimConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new SlimConfigUpdate(instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    protected Writeable.Reader<SlimConfigUpdate> instanceReader() {
        return SlimConfigUpdate::new;
    }

    @Override
    protected SlimConfigUpdate createTestInstance() {
        return randomUpdate();
    }

    @Override
    protected SlimConfigUpdate mutateInstance(SlimConfigUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected SlimConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return SlimConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected SlimConfigUpdate mutateInstanceForVersion(SlimConfigUpdate instance, Version version) {
        return mutateForVersion(instance, version);
    }

    @Override
    Tuple<Map<String, Object>, SlimConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        SlimConfigUpdate expected = new SlimConfigUpdate("ml-results", expectedTokenization);
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    SlimConfigUpdate fromMap(Map<String, Object> map) {
        return SlimConfigUpdate.fromMap(map);
    }
}
