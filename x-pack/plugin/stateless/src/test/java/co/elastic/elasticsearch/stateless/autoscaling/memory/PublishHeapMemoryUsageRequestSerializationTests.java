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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Map;

public class PublishHeapMemoryUsageRequestSerializationTests extends AbstractWireSerializingTestCase<PublishHeapMemoryMetricsRequest> {
    @Override
    protected Writeable.Reader<PublishHeapMemoryMetricsRequest> instanceReader() {
        return PublishHeapMemoryMetricsRequest::new;
    }

    @Override
    protected PublishHeapMemoryMetricsRequest createTestInstance() {
        return new PublishHeapMemoryMetricsRequest(
            new HeapMemoryUsage(
                randomNonNegativeLong(),
                Map.of(
                    new Index(randomAlphaOfLengthBetween(1, 128), randomAlphaOfLengthBetween(1, 128)),
                    new IndexMappingSize(randomNonNegativeLong(), randomAlphaOfLength(64))
                )
            )
        );
    }

    @Override
    protected PublishHeapMemoryMetricsRequest mutateInstance(PublishHeapMemoryMetricsRequest instance) throws IOException {
        return switch (randomInt(1)) {
            case 0 -> new PublishHeapMemoryMetricsRequest(
                new HeapMemoryUsage(randomNonNegativeLong(), instance.getHeapMemoryUsage().indicesMappingSize())
            );
            case 1 -> new PublishHeapMemoryMetricsRequest(
                new HeapMemoryUsage(
                    instance.getHeapMemoryUsage().publicationSeqNo(),
                    Map.of(
                        new Index(randomAlphaOfLengthBetween(1, 128), randomAlphaOfLengthBetween(1, 128)),
                        new IndexMappingSize(randomNonNegativeLong(), randomAlphaOfLength(64))
                    )
                )
            );
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
