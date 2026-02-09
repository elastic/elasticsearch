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

package org.elasticsearch.xpack.stateless.autoscaling.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class PublishIndexingOperationsHeapMemoryRequirementsRequestSerializationTests extends AbstractWireSerializingTestCase<
    TransportPublishIndexingOperationsHeapMemoryRequirements.Request> {
    @Override
    protected Writeable.Reader<TransportPublishIndexingOperationsHeapMemoryRequirements.Request> instanceReader() {
        return TransportPublishIndexingOperationsHeapMemoryRequirements.Request::new;
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request createTestInstance() {
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(randomLongBetween(0, Long.MAX_VALUE));
    }

    @Override
    protected TransportPublishIndexingOperationsHeapMemoryRequirements.Request mutateInstance(
        TransportPublishIndexingOperationsHeapMemoryRequirements.Request instance
    ) throws IOException {
        return new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(
            randomValueOtherThan(instance.getMinimumRequiredHeapInBytes(), () -> randomLongBetween(0, Long.MAX_VALUE))
        );
    }
}
