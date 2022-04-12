/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.cloneWithNewTruncation;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigTestScaffolding.createTokenizationUpdate;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TextClassificationConfigUpdateTests extends AbstractNlpConfigUpdateTestCase<TextClassificationConfigUpdate> {

    @Override
    Tuple<Map<String, Object>, TextClassificationConfigUpdate> fromMapTestInstances(TokenizationUpdate expectedTokenization) {
        int numClasses = randomIntBetween(1, 10);
        TextClassificationConfigUpdate expected = new TextClassificationConfigUpdate(
            List.of("foo", "bar"),
            numClasses,
            "ml-results",
            expectedTokenization
        );
        Map<String, Object> config = new HashMap<>() {
            {
                put(NlpConfig.RESULTS_FIELD.getPreferredName(), "ml-results");
                put(NlpConfig.CLASSIFICATION_LABELS.getPreferredName(), List.of("foo", "bar"));
                put(NlpConfig.NUM_TOP_CLASSES.getPreferredName(), numClasses);
            }
        };
        return Tuple.tuple(config, expected);
    }

    @Override
    TextClassificationConfigUpdate fromMap(Map<String, Object> map) {
        return TextClassificationConfigUpdate.fromMap(map);
    }

    public void testIsNoop() {
        assertTrue(new TextClassificationConfigUpdate.Builder().build().isNoop(TextClassificationConfigTests.createRandom()));

        assertFalse(
            new TextClassificationConfigUpdate.Builder().setResultsField("foo")
                .build()
                .isNoop(
                    new TextClassificationConfig.Builder().setClassificationLabels(List.of("a", "b"))
                        .setNumTopClasses(-1)
                        .setResultsField("bar")
                        .build()
                )
        );

        assertTrue(
            new TextClassificationConfigUpdate.Builder().setNumTopClasses(3)
                .build()
                .isNoop(new TextClassificationConfig.Builder().setClassificationLabels(List.of("a", "b")).setNumTopClasses(3).build())
        );
        assertFalse(
            new TextClassificationConfigUpdate.Builder().setClassificationLabels(List.of("a", "b"))
                .build()
                .isNoop(new TextClassificationConfig.Builder().setClassificationLabels(List.of("c", "d")).build())
        );
        assertFalse(
            new TextClassificationConfigUpdate.Builder().setTokenizationUpdate(
                new BertTokenizationUpdate(Tokenization.Truncate.SECOND, null)
            ).build().isNoop(new TextClassificationConfig.Builder().setClassificationLabels(List.of("c", "d")).build())
        );

    }

    public void testApply() {
        TextClassificationConfig originalConfig = new TextClassificationConfig(
            VocabularyConfigTests.createRandom(),
            BertTokenizationTests.createRandom(),
            List.of("one", "two"),
            randomFrom(-1, randomIntBetween(1, 10)),
            "foo-results"
        );

        assertThat(originalConfig, equalTo(new TextClassificationConfigUpdate.Builder().build().apply(originalConfig)));

        assertThat(
            new TextClassificationConfig.Builder(originalConfig).setClassificationLabels(List.of("foo", "bar")).build(),
            equalTo(
                new TextClassificationConfigUpdate.Builder().setClassificationLabels(List.of("foo", "bar")).build().apply(originalConfig)
            )
        );
        assertThat(
            new TextClassificationConfig.Builder(originalConfig).setResultsField("ml-results").build(),
            equalTo(new TextClassificationConfigUpdate.Builder().setResultsField("ml-results").build().apply(originalConfig))
        );
        assertThat(
            new TextClassificationConfig.Builder(originalConfig).setNumTopClasses(originalConfig.getNumTopClasses() + 2).build(),
            equalTo(
                new TextClassificationConfigUpdate.Builder().setNumTopClasses(originalConfig.getNumTopClasses() + 2)
                    .build()
                    .apply(originalConfig)
            )
        );

        Tokenization.Truncate truncate = randomFrom(Tokenization.Truncate.values());
        Tokenization tokenization = cloneWithNewTruncation(originalConfig.getTokenization(), truncate);
        assertThat(
            new TextClassificationConfig.Builder(originalConfig).setTokenization(tokenization).build(),
            equalTo(
                new TextClassificationConfigUpdate.Builder().setTokenizationUpdate(
                    createTokenizationUpdate(originalConfig.getTokenization(), truncate, null)
                ).build().apply(originalConfig)
            )
        );
    }

    public void testApplyWithInvalidLabels() {
        TextClassificationConfig originalConfig = TextClassificationConfigTests.createRandom();

        int numberNewLabels = originalConfig.getClassificationLabels().size() + 2;
        List<String> newLabels = randomList(numberNewLabels, numberNewLabels, () -> randomAlphaOfLength(6));

        var update = new TextClassificationConfigUpdate.Builder().setClassificationLabels(newLabels).build();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> update.apply(originalConfig));
        assertThat(
            e.getMessage(),
            containsString(
                "The number of [classification_labels] the model is defined with ["
                    + originalConfig.getClassificationLabels().size()
                    + "] does not match the number in the update ["
                    + numberNewLabels
                    + "]"
            )
        );
    }

    @Override
    protected TextClassificationConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return TextClassificationConfigUpdate.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<TextClassificationConfigUpdate> instanceReader() {
        return TextClassificationConfigUpdate::new;
    }

    @Override
    protected TextClassificationConfigUpdate createTestInstance() {
        TextClassificationConfigUpdate.Builder builder = new TextClassificationConfigUpdate.Builder();
        if (randomBoolean()) {
            builder.setNumTopClasses(randomIntBetween(1, 4));
        }
        if (randomBoolean()) {
            builder.setClassificationLabels(randomList(1, 3, () -> randomAlphaOfLength(4)));
        }
        if (randomBoolean()) {
            builder.setResultsField(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            builder.setTokenizationUpdate(new BertTokenizationUpdate(randomFrom(Tokenization.Truncate.values()), null));
        }
        return builder.build();
    }

    @Override
    protected TextClassificationConfigUpdate mutateInstanceForVersion(TextClassificationConfigUpdate instance, Version version) {
        if (version.before(Version.V_8_1_0)) {
            return new TextClassificationConfigUpdate(
                instance.getClassificationLabels(),
                instance.getNumTopClasses(),
                instance.getResultsField(),
                null
            );
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
