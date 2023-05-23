/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlMetadata;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MlMetadataTests extends AbstractChunkedSerializingTestCase<MlMetadata> {

    @Override
    protected MlMetadata createTestInstance() {
        MlMetadata.Builder builder = new MlMetadata.Builder();
        return builder.isResetMode(randomBoolean()).isUpgradeMode(randomBoolean()).build();
    }

    @Override
    protected Writeable.Reader<MlMetadata> instanceReader() {
        return MlMetadata::new;
    }

    @Override
    protected MlMetadata doParseInstance(XContentParser parser) {
        return MlMetadata.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testBuilderClone() {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            MlMetadata first = createTestInstance();
            MlMetadata cloned = MlMetadata.Builder.from(first).build();
            assertThat(cloned, equalTo(first));
        }
    }

    @Override
    protected MlMetadata mutateInstance(MlMetadata instance) {
        boolean isUpgrade = instance.isUpgradeMode();
        boolean isReset = instance.isResetMode();
        MlMetadata.Builder metadataBuilder = new MlMetadata.Builder();

        switch (between(0, 1)) {
            case 0 -> metadataBuilder.isUpgradeMode(isUpgrade == false);
            case 1 -> metadataBuilder.isResetMode(isReset == false);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return metadataBuilder.build();
    }
}
