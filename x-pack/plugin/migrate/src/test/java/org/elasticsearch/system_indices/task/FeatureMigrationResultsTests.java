/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.task;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FeatureMigrationResultsTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.ProjectCustom> {

    private static final ConstructingObjectParser<SingleFeatureMigrationResult, Void> SINGLE_FEATURE_RESULT_PARSER =
        new ConstructingObjectParser<>(
            "feature_migration_status",
            a -> new SingleFeatureMigrationResult((boolean) a[0], (String) a[1], (Exception) a[2])
        );

    static {
        SINGLE_FEATURE_RESULT_PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SingleFeatureMigrationResult.SUCCESS_FIELD);
        SINGLE_FEATURE_RESULT_PARSER.declareString(
            ConstructingObjectParser.optionalConstructorArg(),
            SingleFeatureMigrationResult.FAILED_INDEX_NAME_FIELD
        );
        SINGLE_FEATURE_RESULT_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            SingleFeatureMigrationResult.EXCEPTION_FIELD
        );
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FeatureMigrationResults, Void> PARSER = new ConstructingObjectParser<>(
        FeatureMigrationResults.TYPE,
        a -> {
            final Map<String, SingleFeatureMigrationResult> statuses = ((List<Tuple<String, SingleFeatureMigrationResult>>) a[0]).stream()
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            return new FeatureMigrationResults(statuses);
        }
    );

    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> new Tuple<>(n, SINGLE_FEATURE_RESULT_PARSER.apply(p, c)),
            v -> {
                throw new IllegalArgumentException(
                    "ordered " + FeatureMigrationResults.RESULTS_FIELD.getPreferredName() + " are not supported"
                );
            },
            FeatureMigrationResults.RESULTS_FIELD
        );
    }

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
    protected FeatureMigrationResults mutateInstance(Metadata.ProjectCustom instance) {
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
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return FeatureMigrationResults::new;
    }

    @Override
    protected FeatureMigrationResults doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected Metadata.ProjectCustom makeTestChanges(Metadata.ProjectCustom testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.ProjectCustom>> diffReader() {
        return FeatureMigrationResults.ResultsDiff::new;
    }
}
