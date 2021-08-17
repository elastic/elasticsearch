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

public class BertPassThroughConfigTests extends InferenceConfigItemTestCase<BertPassThroughConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected BertPassThroughConfig doParseInstance(XContentParser parser) throws IOException {
        return lenient ? BertPassThroughConfig.fromXContentLenient(parser) : BertPassThroughConfig.fromXContentStrict(parser);
    }

    @Override
    protected Writeable.Reader<BertPassThroughConfig> instanceReader() {
        return BertPassThroughConfig::new;
    }

    @Override
    protected BertPassThroughConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected BertPassThroughConfig mutateInstanceForVersion(BertPassThroughConfig instance, Version version) {
        return instance;
    }

    public static BertPassThroughConfig createRandom() {
        return new BertPassThroughConfig(
            VocabularyConfigTests.createRandom(),
            randomBoolean() ?
                null :
                randomFrom(BertTokenizationParamsTests.createRandom(), DistilBertTokenizationParamsTests.createRandom())
            );
    }
}
