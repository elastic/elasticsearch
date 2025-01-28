/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

class PrevalidateShardPathRequestSerializationTestUtils {

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }

    public static <T> void mutateList(List<T> list, Supplier<T> supplier) {
        if (list.size() > 0 && randomBoolean()) {
            // just remove one
            list.remove(randomInt(list.size() - 1));
        } else {
            list.add(supplier.get());
        }
    }

    public static <T> Set<T> createSetMutation(Set<T> set, Supplier<T> supplier) {
        List<T> list = new ArrayList<>(set);
        mutateList(list, supplier);
        return new HashSet<>(list);
    }
}
