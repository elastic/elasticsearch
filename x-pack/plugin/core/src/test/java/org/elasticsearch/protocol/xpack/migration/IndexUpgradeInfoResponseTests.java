/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.migration;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexUpgradeInfoResponseTests extends
        AbstractHlrcStreamableXContentTestCase<IndexUpgradeInfoResponse, org.elasticsearch.client.migration.IndexUpgradeInfoResponse> {

    @Override
    public org.elasticsearch.client.migration.IndexUpgradeInfoResponse doHlrcParseInstance(XContentParser parser) {
        return org.elasticsearch.client.migration.IndexUpgradeInfoResponse.fromXContent(parser);
    }

    @Override
    public IndexUpgradeInfoResponse convertHlrcToInternal(org.elasticsearch.client.migration.IndexUpgradeInfoResponse instance) {
        final Map<String, org.elasticsearch.client.migration.UpgradeActionRequired> actions = instance.getActions();
        return new IndexUpgradeInfoResponse(actions.entrySet().stream().map(
            e -> new AbstractMap.SimpleEntry<>(e.getKey(), UpgradeActionRequired.valueOf(e.getValue().name()))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    protected IndexUpgradeInfoResponse createBlankInstance() {
        return new IndexUpgradeInfoResponse();
    }

    @Override
    protected IndexUpgradeInfoResponse createTestInstance() {
        return randomIndexUpgradeInfoResponse(randomIntBetween(0, 10));
    }

    private static IndexUpgradeInfoResponse randomIndexUpgradeInfoResponse(int numIndices) {
        Map<String, UpgradeActionRequired> actions = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            actions.put(randomAlphaOfLength(5), randomFrom(UpgradeActionRequired.values()));
        }
        return new IndexUpgradeInfoResponse(actions);
    }

    @Override
    protected IndexUpgradeInfoResponse mutateInstance(IndexUpgradeInfoResponse instance) {
        if (instance.getActions().size() == 0) {
            return randomIndexUpgradeInfoResponse(1);
        }
        Map<String, UpgradeActionRequired> actions = new HashMap<>(instance.getActions());
        if (randomBoolean()) {
            Iterator<Map.Entry<String, UpgradeActionRequired>> iterator = actions.entrySet().iterator();
            iterator.next();
            iterator.remove();
        } else {
            actions.put(randomAlphaOfLength(5), randomFrom(UpgradeActionRequired.values()));
        }
        return new IndexUpgradeInfoResponse(actions);
    }
}
