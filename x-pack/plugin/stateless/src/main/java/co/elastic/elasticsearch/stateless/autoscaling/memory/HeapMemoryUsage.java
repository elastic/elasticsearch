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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Map;

public record HeapMemoryUsage(long publicationSeqNo, Map<Index, IndexMappingSize> indicesMappingSize) implements Writeable {

    public HeapMemoryUsage(StreamInput in) throws IOException {
        this(in.readVLong(), in.readMap(Index::new, IndexMappingSize::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(publicationSeqNo);
        out.writeMap(indicesMappingSize, (o, index) -> index.writeTo(o), (o, indexMappingSize) -> indexMappingSize.writeTo(o));
    }
}
