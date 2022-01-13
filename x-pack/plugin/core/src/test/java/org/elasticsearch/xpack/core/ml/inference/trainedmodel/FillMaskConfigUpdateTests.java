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

public class FillMaskConfigUpdateTests extends AbstractBWCSerializationTestCase<FillMaskConfigUpdate> {

    public void testFromMap() {
        FillMaskConfigUpdate expected = new FillMaskConfigUpdate(3, "ml-results", new BertTokenizationUpdate(Tokenization.Truncate.FIRST));
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
                put(NlpConfig.NUM_TOP_CLASSES.getPreferredName(), 3);
                Map<String, Object> truncate = new HashMap<>();
                truncate.put("truncate", "first");
                Map<String, Object> bert = new HashMap<>();
                bert.put("bert", truncate);
                put("tokenization", bert);
            }
        };
        var pp = FillMaskConfigUpdate.fromMap(config);
        assertThat(pp, equalTo(expected));
    }

    public void testFromMapWithUnknownField() {
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> FillMaskConfigUpdate.fromMap(Collections.singletonMap("some_key", 1))
        );
        assertThat(ex.getMessage(), equalTo("Unrecognized fields [some_key]."));
    }

    public void testIsNoop() {
        assertTrue(new FillMaskConfigUpdate.Builder().build().isNoop(FillMaskConfigTests.createRandom()));

        assertFalse(
            new FillMaskConfigUpdate.Builder().setResultsField("foo")
                .build()
                .isNoop(new FillMaskConfig.Builder().setResultsField("bar").build())
        );

        assertFalse(
            new FillMaskConfigUpdate.Builder().setTokenizationUpdate(new BertTokenizationUpdate(Tokenization.Truncate.SECOND))
                .build()
                .isNoop(new FillMaskConfig.Builder().setResultsField("bar").build())
        );

        assertTrue(
            new FillMaskConfigUpdate.Builder().setNumTopClasses(3).build().isNoop(new FillMaskConfig.Builder().setNumTopClasses(3).build())
        );
    }

    public void testApply() {
        FillMaskConfig originalConfig = FillMaskConfigTests.createRandom();

        assertThat(originalConfig, equalTo(new FillMaskConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(
            new FillMaskConfig.Builder(originalConfig).setResultsField("ml-results").build(),
            equalTo(new FillMaskConfigUpdate.Builder().setResultsField("ml-results").build().apply(originalConfig))
        );
        assertThat(
            new FillMaskConfig.Builder(originalConfig).setNumTopClasses(originalConfig.getNumTopClasses() + 1).build(),
            equalTo(
                new FillMaskConfigUpdate.Builder().setNumTopClasses(originalConfig.getNumTopClasses() + 1).build().apply(originalConfig)
            )
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new FillMaskConfig.Builder(originalConfig).setTokenization(tokenization).build(),
            equalTo(
                new FillMaskConfigUpdate.Builder().setTokenizationUpdate(
                    createTokenizationUpdate(originalConfig.getTokenization(), truncate)
                ).build().apply(originalConfig)
            )
        );
    }

    @Override
    protected FillMaskConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return FillMaskConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<FillMaskConfigUpdate> instanceReader() {
        return FillMaskConfigUpdate::new;
    }

    @Override
    protected FillMaskConfigUpdate createTestInstance() {
        FillMaskConfigUpdate.Builder builder = new FillMaskConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setNumTopClasses(randomIntBetween(1, 4));
        }
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values())));
        }
        return builder.build();
    }

    @Override
    protected FillMaskConfigUpdate mutateInstanceForVersion(FillMaskConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new FillMaskConfigUpdate(instance.getNumTopClasses(), instance.getResultsField(), null);
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
