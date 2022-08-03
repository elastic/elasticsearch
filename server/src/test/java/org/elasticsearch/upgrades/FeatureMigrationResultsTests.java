/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.SimpleDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class FeatureMigrationResultsTests extends SimpleDiffableSerializationTestCase<Metadata.Custom> {

    @Override
    protected FeatureMigrationResults createTestInstance() {
        return new FeatureMigrationResults(randomMap(0, 10, () -> new Tuple<>(randomAlphaOfLength(5), randomFeatureStatus())));
    }

    private SingleFeatureMigrationResult randomFeatureStatus() {
        if (randomBoolean()) {
            return SingleFeatureMigrationResult.success();
        } else {
            return SingleFeatureMigrationResult.failure(randomAlphaOfLength(8), new RuntimeException(randomAlphaOfLength(5)));
        }
    }

    @Override
    protected FeatureMigrationResults mutateInstance(Metadata.Custom instance) {
        int oldSize = ((FeatureMigrationResults) instance).getFeatureStatuses().size();
        if (oldSize == 0 || randomBoolean()) {
            return new FeatureMigrationResults(
                randomMap(oldSize + 1, oldSize + 5, () -> new Tuple<>(randomAlphaOfLength(5), randomFeatureStatus()))
            );
        } else {
            return new FeatureMigrationResults(randomMap(0, oldSize, () -> new Tuple<>(randomAlphaOfLength(5), randomFeatureStatus())));
        }
    }

    /**
     * Disable assertions of XContent equivalence - the exception prevents this from working as it translates everything
     * into ElasticsearchException.
     */
    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return FeatureMigrationResults::new;
    }

    @Override
    protected FeatureMigrationResults doParseInstance(XContentParser parser) throws IOException {
        return FeatureMigrationResults.fromXContent(parser);
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return FeatureMigrationResults.ResultsDiff::new;
    }
}
