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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class PublishNodeIngestLoadRequestSerializationTests extends AbstractWireSerializingTestCase<PublishNodeIngestLoadRequest> {

    @Override
    protected Writeable.Reader<PublishNodeIngestLoadRequest> instanceReader() {
        return PublishNodeIngestLoadRequest::new;
    }

    @Override
    protected PublishNodeIngestLoadRequest createTestInstance() {
        return new PublishNodeIngestLoadRequest(
            UUIDs.randomBase64UUID(),
            randomIdentifier(),
            randomLong(),
            randomDoubleBetween(0, 512, true)
        );
    }

    @Override
    protected PublishNodeIngestLoadRequest mutateInstance(PublishNodeIngestLoadRequest instance) {
        return switch (randomInt(3)) {
            case 0 -> new PublishNodeIngestLoadRequest(
                randomValueOtherThan(instance.getNodeId(), UUIDs::randomBase64UUID),
                instance.getNodeName(),
                instance.getSeqNo(),
                instance.getIngestionLoad()
            );
            case 1 -> new PublishNodeIngestLoadRequest(
                instance.getNodeId(),
                instance.getNodeName(),
                randomValueOtherThan(instance.getSeqNo(), ESTestCase::randomLong),
                instance.getIngestionLoad()
            );
            case 2 -> new PublishNodeIngestLoadRequest(
                instance.getNodeId(),
                instance.getNodeName(),
                instance.getSeqNo(),
                randomValueOtherThan(instance.getIngestionLoad(), () -> randomDoubleBetween(0, 512, true))
            );
            case 3 -> new PublishNodeIngestLoadRequest(
                instance.getNodeId(),
                randomValueOtherThan(instance.getNodeName(), ESTestCase::randomIdentifier),
                instance.getSeqNo(),
                instance.getIngestionLoad()
            );
            default -> throw new IllegalStateException("Unexpected value: " + randomInt(2));
        };
    }
}
