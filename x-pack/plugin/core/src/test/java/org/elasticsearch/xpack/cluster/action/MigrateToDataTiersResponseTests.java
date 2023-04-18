/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class MigrateToDataTiersResponseTests extends AbstractWireSerializingTestCase<MigrateToDataTiersResponse> {

    @Override
    protected Writeable.Reader<MigrateToDataTiersResponse> instanceReader() {
        return MigrateToDataTiersResponse::new;
    }

    @Override
    protected MigrateToDataTiersResponse createTestInstance() {
        boolean dryRun = randomBoolean();
        return new MigrateToDataTiersResponse(
            randomAlphaOfLength(10),
            randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)),
            randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)),
            randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)),
            randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)),
            randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)),
            dryRun
        );
    }

    @Override
    protected MigrateToDataTiersResponse mutateInstance(MigrateToDataTiersResponse instance) {
        int i = randomIntBetween(0, 6);
        return switch (i) {
            case 0 -> new MigrateToDataTiersResponse(
                randomValueOtherThan(instance.getRemovedIndexTemplateName(), () -> randomAlphaOfLengthBetween(5, 15)),
                instance.getMigratedPolicies(),
                instance.getMigratedIndices(),
                instance.getMigratedLegacyTemplates(),
                instance.getMigratedComposableTemplates(),
                instance.getMigratedComponentTemplates(),
                instance.isDryRun()
            );
            case 1 -> new MigrateToDataTiersResponse(
                instance.getRemovedIndexTemplateName(),
                randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)),
                instance.getMigratedIndices(),
                instance.getMigratedLegacyTemplates(),
                instance.getMigratedComposableTemplates(),
                instance.getMigratedComponentTemplates(),
                instance.isDryRun()
            );
            case 2 -> new MigrateToDataTiersResponse(
                instance.getRemovedIndexTemplateName(),
                instance.getMigratedPolicies(),
                randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)),
                instance.getMigratedLegacyTemplates(),
                instance.getMigratedComposableTemplates(),
                instance.getMigratedComponentTemplates(),
                instance.isDryRun()
            );
            case 3 -> new MigrateToDataTiersResponse(
                instance.getRemovedIndexTemplateName(),
                instance.getMigratedPolicies(),
                instance.getMigratedIndices(),
                randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)),
                instance.getMigratedComposableTemplates(),
                instance.getMigratedComponentTemplates(),
                instance.isDryRun()
            );
            case 4 -> new MigrateToDataTiersResponse(
                instance.getRemovedIndexTemplateName(),
                instance.getMigratedPolicies(),
                instance.getMigratedIndices(),
                instance.getMigratedLegacyTemplates(),
                randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)),
                instance.getMigratedComponentTemplates(),
                instance.isDryRun()
            );
            case 5 -> new MigrateToDataTiersResponse(
                instance.getRemovedIndexTemplateName(),
                instance.getMigratedPolicies(),
                instance.getMigratedIndices(),
                instance.getMigratedComposableTemplates(),
                instance.getMigratedComponentTemplates(),
                randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)),
                instance.isDryRun()
            );
            case 6 -> new MigrateToDataTiersResponse(
                instance.getRemovedIndexTemplateName(),
                instance.getMigratedPolicies(),
                instance.getMigratedIndices(),
                instance.getMigratedLegacyTemplates(),
                instance.getMigratedComposableTemplates(),
                instance.getMigratedComponentTemplates(),
                instance.isDryRun() ? false : true
            );
            default -> throw new UnsupportedOperationException();
        };
    }
}
