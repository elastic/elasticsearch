/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class PassThroughConfigTests extends InferenceConfigItemTestCase<PassThroughConfig> {

    public static PassThroughConfig mutateForVersion(PassThroughConfig instance, Version version) {
        return new PassThroughConfig(
            instance.getVocabularyConfig(),
            InferenceConfigTestScaffolding.mutateTokenizationForVersion(instance.getTokenization(), version),
            instance.getResultsField()
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected PassThroughConfig doParseInstance(XContentParser parser) throws IOException {
        return PassThroughConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<PassThroughConfig> instanceReader() {
        return PassThroughConfig::new;
    }

    @Override
    protected PassThroughConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected PassThroughConfig mutateInstanceForVersion(PassThroughConfig instance, Version version) {
        return mutateForVersion(instance, version);
    }

    public static PassThroughConfig createRandom() {
        return new PassThroughConfig(
            randomBoolean() ? null : VocabularyConfigTests.createRandom(),
            randomBoolean()
                ? null
                : randomFrom(
                    BertTokenizationTests.createRandom(),
                    MPNetTokenizationTests.createRandom(),
                    RobertaTokenizationTests.createRandom()
                ),
            randomBoolean() ? null : randomAlphaOfLength(7)
        );
    }
}
