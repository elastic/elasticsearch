/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;

public class ShardInformationNodeResponseTests extends AbstractWireSerializingTestCase<
    TransportFetchSearchShardInformationAction.Response> {

    @Override
    protected Writeable.Reader<TransportFetchSearchShardInformationAction.Response> instanceReader() {
        return TransportFetchSearchShardInformationAction.Response::new;
    }

    @Override
    protected TransportFetchSearchShardInformationAction.Response createTestInstance() {
        return createLabeledTestInstance("1");
    }

    public static TransportFetchSearchShardInformationAction.Response createLabeledTestInstance(String label) {
        return new TransportFetchSearchShardInformationAction.Response(randomLong());
    }

    @Override
    protected TransportFetchSearchShardInformationAction.Response mutateInstance(
        TransportFetchSearchShardInformationAction.Response instance
    ) {
        return new TransportFetchSearchShardInformationAction.Response(
            randomValueOtherThan(instance.getLastSearcherAcquiredTime(), ESTestCase::randomLong)
        );
    }

    public void testNegativeLongSerialization() throws IOException {
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            TransportFetchSearchShardInformationAction.SHARD_HAS_MOVED_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
        try (BytesStreamOutput bos = new BytesStreamOutput()) {
            TransportFetchSearchShardInformationAction.NO_OTHER_SHARDS_FOUND_RESPONSE.writeTo(bos);
            assertThat(bos.size(), greaterThan(0));
        }
    }
}
