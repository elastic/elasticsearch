/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class PassThroughConfigUpdateTests extends AbstractBWCSerializationTestCase<PassThroughConfigUpdate> {

    public void testFromMap() {
        PassThroughConfigUpdate expected = new PassThroughConfigUpdate(
            "ml-results",
            new BertTokenizationUpdate(Tokenization.Truncate.FIRST)
        );
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
                Map<String, Object> truncate = new HashMap<>();
                truncate.put("truncate", "first");
                Map<String, Object> bert = new HashMap<>();
                bert.put("bert", truncate);
                put("tokenization", bert);
            }
        };
        assertThat(PassThroughConfigUpdate.fromMap(config), equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> PassThroughConfigUpdate.fromMap(Collections.singletonMap("some_key", 1))
        );
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testApply() {
        PassThroughConfig originalConfig = PassThroughConfigTests.createRandom();

        assertThat(originalConfig, sameInstance(new PassThroughConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(
            new PassThroughConfig(originalConfig.getVocabularyConfig(), originalConfig.getTokenization(), "ml-results"),
            equalTo(new PassThroughConfigUpdate.Builder().setResultsField("ml-results").build().apply(originalConfig))
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new PassThroughConfig(originalConfig.getVocabularyConfig(), tokenization, originalConfig.getResultsField()),
            equalTo(
                new PassThroughConfigUpdate.Builder().setTokenizationUpdate(
                    createTokenizationUpdate(originalConfig.getTokenization(), truncate)
                ).build().apply(originalConfig)
            )
        );
    }

    @Override
    protected PassThroughConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return PassThroughConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<PassThroughConfigUpdate> instanceReader() {
        return PassThroughConfigUpdate::new;
    }

    @Override
    protected PassThroughConfigUpdate createTestInstance() {
        PassThroughConfigUpdate.Builder builder = new PassThroughConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values())));
        }
        return builder.build();
    }

    @Override
    protected PassThroughConfigUpdate mutateInstanceForVersion(PassThroughConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new PassThroughConfigUpdate(instance.getResultsField(), null);
        }
        return instance;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }
}
