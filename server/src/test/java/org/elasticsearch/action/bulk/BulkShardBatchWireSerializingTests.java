/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BulkShardBatchWireSerializingTests extends AbstractWireSerializingTestCase<BulkShardBatch> {

    @Override
    protected Writeable.Reader<BulkShardBatch> instanceReader() {
        return BulkShardBatch::new;
    }

    @Override
    protected BulkShardBatch createTestInstance() {
        return randomBulkShardBatch(randomIntBetween(1, 16));
    }

    @Override
    protected BulkShardBatch mutateInstance(BulkShardBatch instance) throws IOException {
        // Re-encode with a different document count to guarantee different bytes.
        int originalDocCount = instance.getEirfBatch().docCount();
        int newDocCount = randomValueOtherThan(originalDocCount, () -> randomIntBetween(1, 16));
        return randomBulkShardBatch(newDocCount);
    }

    private static BulkShardBatch randomBulkShardBatch(int docCount) {
        List<BytesReference> sources = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            sources.add(randomJsonDoc(i));
        }
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, XContentType.JSON, 0);
            }
            EirfBatch batch = encoder.buildPartition(0);
            return new BulkShardBatch(batch);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static BytesReference randomJsonDoc(int seed) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("name", "doc-" + seed + "-" + randomAlphaOfLengthBetween(3, 10));
            builder.field("value", randomLong());
            builder.field("flag", randomBoolean());
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

}
