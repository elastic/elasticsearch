/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.ScriptContextInfo;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetScriptContextResponseTests extends AbstractXContentSerializingTestCase<GetScriptContextResponse> {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetScriptContextResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_script_context",
        true,
        (a) -> {
            Map<String, ScriptContextInfo> contexts = ((List<ScriptContextInfo>) a[0]).stream()
                .collect(Collectors.toMap(ScriptContextInfo::getName, c -> c));
            return new GetScriptContextResponse(contexts);
        }
    );

    static {
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            ScriptContextInfo.PARSER::apply,
            GetScriptContextResponse.CONTEXTS
        );
    }

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
        return PARSER.apply(parser, null);
    }

    @Override
    protected GetScriptContextResponse mutateInstance(GetScriptContextResponse instance) {
        return new GetScriptContextResponse(ScriptContextInfoSerializingTests.mutateOne(instance.contexts.values()));
    }
}
