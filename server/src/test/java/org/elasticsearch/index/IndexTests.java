/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class IndexTests extends AbstractXContentSerializingTestCase<Index> {

    @Override
    protected Writeable.Reader<Index> instanceReader() {
        return Index::new;
    }

    @Override
    protected Index doParseInstance(XContentParser parser) throws IOException {
        return Index.fromXContent(parser);
    }

    @Override
    protected Index createTestInstance() {
        return new Index(randomIdentifier(), UUIDs.randomBase64UUID());
    }

    @Override
    protected Index mutateInstance(Index instance) {
        return mutate(instance);
    }

    public static Index mutate(Index instance) {
        return switch (randomInt(1)) {
            case 0 -> new Index(randomValueOtherThan(instance.getName(), ESTestCase::randomIdentifier), instance.getUUID());
            case 1 -> new Index(instance.getName(), randomValueOtherThan(instance.getUUID(), UUIDs::randomBase64UUID));
            default -> throw new RuntimeException("unreachable");
        };
    }

    public void testToString() {
        assertEquals("[name/uuid]", new Index("name", "uuid").toString());
        assertEquals("[name]", new Index("name", ClusterState.UNKNOWN_UUID).toString());

        Index random = new Index(
            randomSimpleString(random(), 1, 100),
            usually() ? UUIDs.randomBase64UUID(random()) : ClusterState.UNKNOWN_UUID
        );
        assertThat(random.toString(), containsString(random.getName()));
        if (ClusterState.UNKNOWN_UUID.equals(random.getUUID())) {
            assertThat(random.toString(), not(containsString(random.getUUID())));
        } else {
            assertThat(random.toString(), containsString(random.getUUID()));
        }
    }
}
