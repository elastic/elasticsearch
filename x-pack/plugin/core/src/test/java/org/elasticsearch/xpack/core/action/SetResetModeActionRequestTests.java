/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

public class SetResetModeActionRequestTests extends AbstractXContentSerializingTestCase<SetResetModeActionRequest> {

    @Override
    protected SetResetModeActionRequest createTestInstance() {
        boolean enabled = randomBoolean();
        return new SetResetModeActionRequest(TEST_REQUEST_TIMEOUT, enabled, enabled == false && randomBoolean());
    }

    @Override
    protected SetResetModeActionRequest mutateInstance(SetResetModeActionRequest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<SetResetModeActionRequest> instanceReader() {
        return SetResetModeActionRequest::new;
    }

    @Override
    protected SetResetModeActionRequest doParseInstance(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static final ConstructingObjectParser<SetResetModeActionRequest, Void> PARSER = new ConstructingObjectParser<>(
        "set_reset_mode_action_request",
        a -> new SetResetModeActionRequest(TEST_REQUEST_TIMEOUT, (Boolean) a[0], (Boolean) a[1])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField(SetResetModeActionRequest.ENABLED_FIELD_NAME));
        PARSER.declareBoolean(
            ConstructingObjectParser.optionalConstructorArg(),
            new ParseField(SetResetModeActionRequest.DELETE_METADATA_FIELD_NAME)
        );
    }

}
