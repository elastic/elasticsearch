/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class RelocationFailureInfoTests extends AbstractWireSerializingTestCase<RelocationFailureInfo> {

    @Override
    protected Writeable.Reader<RelocationFailureInfo> instanceReader() {
        return RelocationFailureInfo::readFrom;
    }

    @Override
    protected RelocationFailureInfo createTestInstance() {
        return new RelocationFailureInfo(randomIntBetween(1, 10));
    }

    @Override
    protected RelocationFailureInfo mutateInstance(RelocationFailureInfo instance) {
        return new RelocationFailureInfo(instance.failedRelocations() + 1);
    }
}
