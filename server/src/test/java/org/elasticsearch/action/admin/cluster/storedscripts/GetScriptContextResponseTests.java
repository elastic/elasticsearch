/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

public class GetScriptContextResponseTests extends AbstractXContentSerializingTestCase<GetScriptContextResponse> {

    @Override
    protected GetScriptContextResponse createTestInstance() {
        if (randomBoolean()) {
            return new GetScriptContextResponse(Collections.emptySet());
        }
        return new GetScriptContextResponse(ScriptContextInfoSerializingTests.randomInstances());
    }

    @Override
    protected Writeable.Reader<GetScriptContextResponse> instanceReader() {
        return GetScriptContextResponse::new;
    }

    @Override
    protected GetScriptContextResponse doParseInstance(XContentParser parser) throws IOException {
        return GetScriptContextResponse.fromXContent(parser);
    }

    @Override
    protected GetScriptContextResponse mutateInstance(GetScriptContextResponse instance) {
        return new GetScriptContextResponse(ScriptContextInfoSerializingTests.mutateOne(instance.contexts.values()));
    }
}
