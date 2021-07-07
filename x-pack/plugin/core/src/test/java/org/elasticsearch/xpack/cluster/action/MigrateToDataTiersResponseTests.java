/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class MigrateToDataTiersResponseTests extends AbstractWireSerializingTestCase<MigrateToDataTiersResponse> {

    @Override
    protected Writeable.Reader<MigrateToDataTiersResponse> instanceReader() {
        return MigrateToDataTiersResponse::new;
    }

    @Override
    protected MigrateToDataTiersResponse createTestInstance() {
        boolean dryRun = randomBoolean();
        return new MigrateToDataTiersResponse(randomAlphaOfLength(10), randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)),
            randomList(1, 5, () -> randomAlphaOfLengthBetween(5, 50)), dryRun);
    }

    @Override
    protected MigrateToDataTiersResponse mutateInstance(MigrateToDataTiersResponse instance) throws IOException {
        int i = randomIntBetween(0, 3);
        switch (i) {
            case 0:
                return new MigrateToDataTiersResponse(randomValueOtherThan(instance.getRemovedIndexTemplateName(),
                    () -> randomAlphaOfLengthBetween(5, 15)), instance.getMigratedPolicies(), instance.getMigratedIndices(),
                    instance.isDryRun());
            case 1:
                return new MigrateToDataTiersResponse(instance.getRemovedIndexTemplateName(),
                    randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)), instance.getMigratedIndices(), instance.isDryRun());
            case 2:
                return new MigrateToDataTiersResponse(instance.getRemovedIndexTemplateName(), instance.getMigratedPolicies(),
                    randomList(6, 10, () -> randomAlphaOfLengthBetween(5, 50)), instance.isDryRun());
            case 3:
                return new MigrateToDataTiersResponse(instance.getRemovedIndexTemplateName(), instance.getMigratedPolicies(),
                    instance.getMigratedIndices(), instance.isDryRun() ? false : true);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
