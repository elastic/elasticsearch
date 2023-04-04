
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.test.AbstractXContentSerializingTestCase;

import java.io.IOException;

public abstract class RankBuilderTests<RCB extends RankBuilder<RCB>> extends AbstractXContentSerializingTestCase<RCB> {

    @Override
    protected final RCB createTestInstance() {
        RCB builder = doCreateTestInstance();
        if (frequently()) {
            builder.windowSize(randomIntBetween(0, 10000));
        }
        return builder;
    }

    protected abstract RCB doCreateTestInstance();

    @Override
    protected final RCB mutateInstance(RCB instance) throws IOException {
        RCB builder = doMutateInstance(instance);
        builder.windowSize(builder.windowSize() == 0 ? 1 : builder.windowSize() - 1);
        return builder;
    }

    protected abstract RCB doMutateInstance(RCB instance) throws IOException;
}
