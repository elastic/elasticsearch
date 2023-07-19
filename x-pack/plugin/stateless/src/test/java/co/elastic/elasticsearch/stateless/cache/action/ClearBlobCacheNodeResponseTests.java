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

package co.elastic.elasticsearch.stateless.cache.action;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ClearBlobCacheNodeResponseTests extends AbstractWireSerializingTestCase<ClearBlobCacheNodeResponse> {
    static final int MAX_CACHE_ENTRIES_CLEARED = 5000;

    @Override
    protected Writeable.Reader<ClearBlobCacheNodeResponse> instanceReader() {
        return ClearBlobCacheNodeResponse::new;
    }

    @Override
    protected ClearBlobCacheNodeResponse createTestInstance() {
        return createLabeledTestInstance("1");
    }

    public static List<ClearBlobCacheNodeResponse> createTestInstances(int numInstances) {
        return IntStream.range(0, numInstances).mapToObj(instanceNum -> createLabeledTestInstance(Integer.toString(instanceNum))).toList();
    }

    public static ClearBlobCacheNodeResponse createLabeledTestInstance(String label) {
        return new ClearBlobCacheNodeResponse(
            DiscoveryNodeUtils.builder("node_" + label).roles(emptySet()).build(),
            randomNonNegativeLong(),
            randomInt(MAX_CACHE_ENTRIES_CLEARED)
        );
    }

    @Override
    public ClearBlobCacheNodeResponse mutateInstance(ClearBlobCacheNodeResponse instance) {
        return switch (between(0, 2)) {
            case 0 -> new ClearBlobCacheNodeResponse(
                DiscoveryNodeUtils.builder("mutated").roles(emptySet()).build(),
                instance.getTimestamp(),
                instance.getEvictions()
            );
            case 1 -> new ClearBlobCacheNodeResponse(
                instance.getNode(),
                randomValueOtherThan(instance.getTimestamp(), ESTestCase::randomNonNegativeLong),
                instance.getEvictions()
            );
            case 2 -> new ClearBlobCacheNodeResponse(
                instance.getNode(),
                instance.getTimestamp(),
                randomValueOtherThan(instance.getEvictions(), () -> randomInt(MAX_CACHE_ENTRIES_CLEARED))
            );
            default -> throw new AssertionError();
        };
    }

    public static final ConstructingObjectParser<ClearBlobCacheNodeResponse, Void> PARSER = new ConstructingObjectParser<>(
        "clear_cache_details",
        true,
        (Object[] parsedObjects) -> {
            return new ClearBlobCacheNodeResponse(
                DiscoveryNodeUtils.create((String) parsedObjects[0]),
                (long) parsedObjects[1],
                (int) parsedObjects[2]
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("node_id"));
        PARSER.declareLong(constructorArg(), new ParseField("timestamp"));
        PARSER.declareInt(constructorArg(), new ParseField("cache_entries_cleared"));
    }
}
