/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.InferenceConfigItemTestCase;
import org.junit.Before;

import java.io.IOException;

public class PassThroughConfigTests extends InferenceConfigItemTestCase<PassThroughConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected PassThroughConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? PassThroughConfig.fromXContentLenient(parser) : PassThroughConfig.fromXContentStrict(parser);
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
        return instance;
    }

    public static PassThroughConfig createRandom() {
        return new PassThroughConfig(
            VocabularyConfigTests.createRandom(),
            randomBoolean() ?
                null :
                randomFrom(BertTokenizationTests.createRandom(), DistilBertTokenizationTests.createRandom())
            );
    }
}
