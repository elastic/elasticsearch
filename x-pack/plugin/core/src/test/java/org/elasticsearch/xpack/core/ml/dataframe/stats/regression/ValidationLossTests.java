/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.regression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.FoldValuesTests;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ValidationLossTests extends AbstractBWCSerializationTestCase<ValidationLoss> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected ValidationLoss doParseInstance(XContentParser parser) throws IOException {
        return ValidationLoss.fromXContent(parser, lenient);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected Writeable.Reader<ValidationLoss> instanceReader() {
        return ValidationLoss::new;
    }

    @Override
    protected ValidationLoss createTestInstance() {
        return createRandom();
    }

    @Override
    protected ValidationLoss mutateInstance(ValidationLoss instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static ValidationLoss createRandom() {
        return new ValidationLoss(randomAlphaOfLength(10), randomList(5, FoldValuesTests::createRandom));
    }

    @Override
    protected ValidationLoss mutateInstanceForVersion(ValidationLoss instance, TransportVersion version) {
        return instance;
    }

    public void testValidationLossForStats() {
        String foldValuesFieldName = ValidationLoss.FOLD_VALUES.getPreferredName();
        ValidationLoss validationLoss = createTestInstance();

        // FOR_INTERNAL_STORAGE param defaults to "false", fold values *not* outputted
        assertThat(Strings.toString(validationLoss), not(containsString(foldValuesFieldName)));

        // FOR_INTERNAL_STORAGE param explicitly set to "false", fold values *not* outputted
        assertThat(
            Strings.toString(
                validationLoss,
                new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "false"))
            ),
            not(containsString(foldValuesFieldName))
        );

        // FOR_INTERNAL_STORAGE param explicitly set to "true", fold values are outputted
        assertThat(
            Strings.toString(
                validationLoss,
                new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"))
            ),
            containsString(foldValuesFieldName)
        );
    }
}
