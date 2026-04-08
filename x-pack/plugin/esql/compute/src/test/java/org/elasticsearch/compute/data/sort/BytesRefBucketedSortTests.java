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
import org.elasticsearch.common.bytes.PagedBytesBuilder;
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

    public void testCollectPagedGatherMode() {
        BigArrays bigArrays = bigArrays();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var values = threeSortedValues();
        try (
            BytesRefBucketedSort sort = build(SortOrder.DESC, 3);
            PagedBytesBuilder b0 = new PagedBytesBuilder(bigArrays.recycler(), breaker, "test", values.get(0).length);
            PagedBytesBuilder b1 = new PagedBytesBuilder(bigArrays.recycler(), breaker, "test", values.get(1).length);
            PagedBytesBuilder b2 = new PagedBytesBuilder(bigArrays.recycler(), breaker, "test", values.get(2).length)
        ) {
            b0.append(values.get(0).bytes, values.get(0).offset, values.get(0).length);
            b1.append(values.get(1).bytes, values.get(1).offset, values.get(1).length);
            b2.append(values.get(2).bytes, values.get(2).offset, values.get(2).length);
            sort.collect(b0, 0);
            sort.collect(b1, 0);
            sort.collect(b2, 0);
            assertBlock(sort, 0, List.of(values.get(2), values.get(1), values.get(0)));
        }
    }

    public void testCollectPagedCompetitive() {
        BigArrays bigArrays = bigArrays();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var values = threeSortedValues();
        try (
            BytesRefBucketedSort sort = build(SortOrder.DESC, 1);
            PagedBytesBuilder small = new PagedBytesBuilder(bigArrays.recycler(), breaker, "test", values.get(0).length);
            PagedBytesBuilder large = new PagedBytesBuilder(bigArrays.recycler(), breaker, "test", values.get(2).length)
        ) {
            small.append(values.get(0).bytes, values.get(0).offset, values.get(0).length);
            large.append(values.get(2).bytes, values.get(2).offset, values.get(2).length);
            sort.collect(small, 0);   // gathered, becomes root
            sort.collect(large, 0);   // larger — should replace root
            assertBlock(sort, 0, List.of(values.get(2)));
        }
    }
}
