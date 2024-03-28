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

package co.elastic.elasticsearch.stateless.autoscaling.search.load;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class PublishNodeSearchLoadRequestSerializationTests extends AbstractWireSerializingTestCase<PublishNodeSearchLoadRequest> {

    @Override
    protected Writeable.Reader<PublishNodeSearchLoadRequest> instanceReader() {
        return PublishNodeSearchLoadRequest::new;
    }

    @Override
    protected PublishNodeSearchLoadRequest createTestInstance() {
        return new PublishNodeSearchLoadRequest(UUIDs.randomBase64UUID(), randomLong(), randomDoubleBetween(0, 512, true));
    }

    @Override
    protected PublishNodeSearchLoadRequest mutateInstance(PublishNodeSearchLoadRequest instance) {
        return switch (randomInt(2)) {
            case 0 -> new PublishNodeSearchLoadRequest(
                randomValueOtherThan(instance.getNodeId(), UUIDs::randomBase64UUID),
                instance.getSeqNo(),
                instance.getSearchLoad()
            );
            case 1 -> new PublishNodeSearchLoadRequest(
                instance.getNodeId(),
                randomValueOtherThan(instance.getSeqNo(), ESTestCase::randomLong),
                instance.getSearchLoad()
            );
            case 2 -> new PublishNodeSearchLoadRequest(
                instance.getNodeId(),
                instance.getSeqNo(),
                randomValueOtherThan(instance.getSearchLoad(), () -> randomDoubleBetween(0, 512, true))
            );
            default -> throw new IllegalStateException("Unexpected value: " + randomInt(2));
        };
    }
}
