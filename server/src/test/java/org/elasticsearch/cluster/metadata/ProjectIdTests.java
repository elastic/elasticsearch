/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ProjectIdTests extends AbstractWireSerializingTestCase<ProjectId> {

    @Override
    protected Writeable.Reader<ProjectId> instanceReader() {
        return ProjectId.READER;
    }

    @Override
    protected ProjectId createTestInstance() {
        return new ProjectId(randomUUID());
    }

    @Override
    protected ProjectId mutateInstance(ProjectId instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
