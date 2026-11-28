/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.synonyms.SynonymRule;
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
        // Response does not expose the rule. The wire form is a single SynonymRule.
        SynonymRule rule;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                rule = new SynonymRule(in);
            }
        }
        return switch (between(0, 1)) {
            case 0 -> new GetSynonymRuleAction.Response(
                new SynonymRule(randomValueOtherThan(rule.id(), () -> randomAlphaOfLengthBetween(1, 10)), rule.synonyms())
            );
            case 1 -> new GetSynonymRuleAction.Response(
                new SynonymRule(rule.id(), randomValueOtherThan(rule.synonyms(), () -> randomAlphaOfLengthBetween(1, 20)))
            );
            default -> throw new AssertionError("illegal mutate branch");
        };
    }
}
