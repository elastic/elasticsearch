/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymRule;

public class GetSynonymRuleActionResponseSerializingTests extends AbstractWireSerializingTestCase<GetSynonymRuleAction.Response> {

    @Override
    protected Writeable.Reader<GetSynonymRuleAction.Response> instanceReader() {
        return GetSynonymRuleAction.Response::new;
    }

    @Override
    protected GetSynonymRuleAction.Response createTestInstance() {
        return new GetSynonymRuleAction.Response(randomSynonymRule());
    }

    @Override
    protected GetSynonymRuleAction.Response mutateInstance(GetSynonymRuleAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
