/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;

public class GetSynonymsActionResponseSerializingTests extends AbstractWireSerializingTestCase<GetSynonymsAction.Response> {

    @Override
    protected Writeable.Reader<GetSynonymsAction.Response> instanceReader() {
        return GetSynonymsAction.Response::new;
    }

    @Override
    protected GetSynonymsAction.Response createTestInstance() {
        SynonymRule[] rules = randomSynonymsSet();
        long total = randomLongBetween(0, Long.MAX_VALUE);
        if (randomBoolean()) {
            // legacy response: no cursor
            return new GetSynonymsAction.Response(new PagedResult<>(total, rules));
        } else {
            // cursor-based response: nextSearchAfter may be null (last page) or a rule ID
            String nextSearchAfter = randomBoolean() ? null : randomIdentifier();
            return new GetSynonymsAction.Response(
                new SynonymsManagementAPIService.SynonymRulesPage(new PagedResult<>(total, rules), nextSearchAfter)
            );
        }
    }

    @Override
    protected GetSynonymsAction.Response mutateInstance(GetSynonymsAction.Response instance) throws IOException {
        PagedResult<SynonymRule> results = instance.getResults();
        String nextSearchAfter = instance.nextSearchAfter();
        switch (between(0, 1)) {
            case 0 -> results = randomValueOtherThan(
                results,
                () -> new PagedResult<>(randomLongBetween(0, Long.MAX_VALUE), randomSynonymsSet())
            );
            case 1 -> nextSearchAfter = randomValueOtherThan(nextSearchAfter, () -> randomBoolean() ? null : randomIdentifier());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new GetSynonymsAction.Response(new SynonymsManagementAPIService.SynonymRulesPage(results, nextSearchAfter));
    }
}
