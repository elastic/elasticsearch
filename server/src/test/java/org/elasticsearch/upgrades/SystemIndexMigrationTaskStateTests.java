/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Map;

public class SystemIndexMigrationTaskStateTests extends AbstractNamedWriteableTestCase<SystemIndexMigrationTaskState> {

    @Override
    protected SystemIndexMigrationTaskState createTestInstance() {
        return randomSystemIndexMigrationTask();
    }

    static SystemIndexMigrationTaskState randomSystemIndexMigrationTask() {
        return new SystemIndexMigrationTaskState(
            randomAlphaOfLengthBetween(5, 20),
            randomAlphaOfLengthBetween(5, 20),
            randomFeatureCallbackMetadata()
        );
    }

    private static Map<String, Object> randomFeatureCallbackMetadata() {
        return randomMap(0, 10, () -> new Tuple<>(randomAlphaOfLength(5), randomMetadataValue()));
    }

    private static Object randomMetadataValue() {
        switch (randomIntBetween(0, 5)) {
            case 0:
                return randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(5), randomMetadataValue()));
            case 1:
                return randomList(0, 3, SystemIndexMigrationTaskStateTests::randomMetadataValue);
            case 2:
                return randomLong();
            case 3:
                return randomBoolean();
            case 4:
                return randomDouble();
            case 5:
                return randomAlphaOfLengthBetween(5, 10);
        }
        throw new AssertionError("bad randomization");
    }

    @Override
    protected SystemIndexMigrationTaskState copyInstance(SystemIndexMigrationTaskState instance, Version version) throws IOException {
        return new SystemIndexMigrationTaskState(
            instance.getCurrentIndex(),
            instance.getCurrentFeature(),
            instance.getFeatureCallbackMetadata()
        );
    }

    @Override
    protected SystemIndexMigrationTaskState mutateInstance(SystemIndexMigrationTaskState instance) {
        String index = instance.getCurrentIndex();
        String feature = instance.getCurrentFeature();
        Map<String, Object> featureMetadata = instance.getFeatureCallbackMetadata();
        switch (randomIntBetween(0, 2)) {
            case 0:
                index = randomValueOtherThan(instance.getCurrentIndex(), () -> randomAlphaOfLengthBetween(5, 20));
                break;
            case 1:
                feature = randomValueOtherThan(instance.getCurrentFeature(), () -> randomAlphaOfLengthBetween(5, 20));
                break;
            case 2:
                featureMetadata = randomValueOtherThan(
                    instance.getFeatureCallbackMetadata(),
                    SystemIndexMigrationTaskStateTests::randomFeatureCallbackMetadata
                );
                break;
            default:
                assert false : "invalid randomization case";
        }
        return new SystemIndexMigrationTaskState(index, feature, featureMetadata);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            SystemIndexMigrationExecutor.getNamedWriteables()

        );
    }

    @Override
    protected Class<SystemIndexMigrationTaskState> categoryClass() {
        return SystemIndexMigrationTaskState.class;
    }
}
