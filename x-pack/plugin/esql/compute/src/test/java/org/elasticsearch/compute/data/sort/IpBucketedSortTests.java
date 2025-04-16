/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IpBucketedSortTests extends BucketedSortTestCase<IpBucketedSort, BytesRef> {
    @Override
    protected IpBucketedSort build(SortOrder sortOrder, int bucketSize) {
        return new IpBucketedSort(bigArrays(), sortOrder, bucketSize);
    }

    @Override
    protected BytesRef randomValue() {
        return new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
    }

    @Override
    protected List<BytesRef> threeSortedValues() {
        return List.of(
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::"))),
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1"))),
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("9999::")))
        );
    }

    @Override
    protected void collect(IpBucketedSort sort, BytesRef value, int bucket) {
        sort.collect(value, bucket);
    }

    @Override
    protected void merge(IpBucketedSort sort, int groupId, IpBucketedSort other, int otherGroupId) {
        sort.merge(groupId, other, otherGroupId);
    }

    @Override
    protected Block toBlock(IpBucketedSort sort, BlockFactory blockFactory, IntVector selected) {
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
