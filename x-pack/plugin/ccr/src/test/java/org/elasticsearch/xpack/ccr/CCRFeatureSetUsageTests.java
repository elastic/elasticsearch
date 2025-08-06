/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class CCRFeatureSetUsageTests extends AbstractWireSerializingTestCase<TransportCCRInfoAction.Usage> {

    @Override
    protected TransportCCRInfoAction.Usage createTestInstance() {
        return new TransportCCRInfoAction.Usage(
            randomBoolean(),
            randomBoolean(),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomNonNegativeLong()
        );
    }

    @Override
    protected TransportCCRInfoAction.Usage mutateInstance(TransportCCRInfoAction.Usage instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<TransportCCRInfoAction.Usage> instanceReader() {
        return TransportCCRInfoAction.Usage::new;
    }
}
