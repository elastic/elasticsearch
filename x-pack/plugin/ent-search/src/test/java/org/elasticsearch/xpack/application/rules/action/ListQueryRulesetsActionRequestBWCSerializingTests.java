/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;

public class ListQueryRulesetsActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<ListQueryRulesetsAction.Request> {

    @Override
    protected Writeable.Reader<ListQueryRulesetsAction.Request> instanceReader() {
        return ListQueryRulesetsAction.Request::new;
    }

    @Override
    protected ListQueryRulesetsAction.Request createTestInstance() {

        PageParams pageParams = EnterpriseSearchModuleTestUtils.randomPageParams();
        return new ListQueryRulesetsAction.Request(pageParams);
    }

    @Override
    protected ListQueryRulesetsAction.Request mutateInstance(ListQueryRulesetsAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListQueryRulesetsAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ListQueryRulesetsAction.Request.parse(parser);
    }

    @Override
    protected ListQueryRulesetsAction.Request mutateInstanceForVersion(ListQueryRulesetsAction.Request instance, TransportVersion version) {
        return new ListQueryRulesetsAction.Request(instance.pageParams());
    }
}
