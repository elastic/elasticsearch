/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BytesRefBucketedSortTests extends BucketedSortTestCase<BytesRefBucketedSort, BytesRef> {
    @Override
    protected BytesRefBucketedSort build(SortOrder sortOrder, int bucketSize) {
        BigArrays bigArrays = bigArrays();
        return new BytesRefBucketedSort(
            bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST),
            "test",
            bigArrays,
            sortOrder,
            bucketSize
        );
    }

    @Override
    protected BytesRef randomValue() {
        return new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
    }

    @Override
    protected List<BytesRef> threeSortedValues() {
        List<BytesRef> values = new ArrayList<>();
        values.add(new BytesRef(randomAlphaOfLength(10)));
        values.add(new BytesRef(randomAlphaOfLength(11)));
        values.add(new BytesRef(randomAlphaOfLength(1)));
        Collections.sort(values);
        return values;
    }

    @Override
    protected void collect(BytesRefBucketedSort sort, BytesRef value, int bucket) {
        sort.collect(value, bucket);
    }

    @Override
    protected void merge(BytesRefBucketedSort sort, int groupId, BytesRefBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(BytesRefBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
        return sort.toBlock(blockFactory, selected);
    }

    @Override
    protected void assertBlockTypeAndValues(Block block, List<BytesRef> values) {
        assertThat(block.elementType(), equalTo(ElementType.BYTES_REF));
        var typedBlock = (BytesRefBlock) block;
        var scratch = new BytesRef();
        for (int i = 0; i < values.size(); i++) {
            assertThat("expected value on block position " + i, typedBlock.getBytesRef(i, scratch), equalTo(values.get(i)));
        }
    }
}
