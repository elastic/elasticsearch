/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DataStreamAliasTests extends AbstractSerializingTestCase<DataStreamAlias> {

    @Override
    protected DataStreamAlias doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        parser.nextToken();
        return DataStreamAlias.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStreamAlias> instanceReader() {
        return DataStreamAlias::new;
    }

    @Override
    protected DataStreamAlias createTestInstance() {
        return DataStreamTestHelper.randomAliasInstance();
    }

    public void testAddDataStream() {
        // Add ds-3 to alias, a different instance is returned with ds-3 as one of the data streams being referred to
        DataStreamAlias alias = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), null);
        DataStreamAlias result = alias.addDataStream("ds-3", null);
        assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-2", "ds-3"));
        assertThat(result.getWriteDataStream(), nullValue());
        // Add ds-3 to alias as write data stream, same as above but the returned instance also refers to ds-3 as write data stream
        result = alias.addDataStream("ds-3", true);
        assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-2", "ds-3"));
        assertThat(result.getWriteDataStream(), equalTo("ds-3"));
        // Add ds-2 as data stream, which is already referred to by this alias. The same instance is returned to signal a noop.
        result = alias.addDataStream("ds-2", null);
        assertThat(result, sameInstance(alias));
        // Add ds-2 as non write data stream, which is already referred to by this alias. The same instance is returned to signal a noop.
        result = alias.addDataStream("ds-2", false);
        assertThat(result, sameInstance(alias));
        // Add ds-2 as write data stream, which is already referred to by this alias, but not as write data stream,
        // an updated instance should be returned.
        result = alias.addDataStream("ds-2", true);
        assertThat(result, not(sameInstance(alias)));
        assertThat(result.getWriteDataStream(), equalTo("ds-2"));
        // Add ds-2 as write non data stream, which is already referred to by this alias, but as write data stream,
        // an updated instance should be returned.
        alias = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), "ds-2");
        result = alias.addDataStream("ds-2", false);
        assertThat(result, not(sameInstance(alias)));
        assertThat(result.getWriteDataStream(), nullValue());
    }

    public void testRemoveDataStream() {
        // Remove a referenced data stream:
        DataStreamAlias alias = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), null);
        DataStreamAlias result = alias.removeDataStream("ds-2");
        assertThat(result, not(sameInstance(alias)));
        assertThat(result.getDataStreams(), containsInAnyOrder("ds-1"));
        assertThat(result.getWriteDataStream(), nullValue());
        // Remove the data stream that is also referenced as write data stream:
        alias = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), "ds-2");
        result = alias.removeDataStream("ds-2");
        assertThat(result, not(sameInstance(alias)));
        assertThat(result.getDataStreams(), containsInAnyOrder("ds-1"));
        assertThat(result.getWriteDataStream(), nullValue());
        // Removing the last referenced data stream name, return null to signal the entire alias can be removed.
        alias = new DataStreamAlias("my-alias", List.of("ds-1"), null);
        result = alias.removeDataStream("ds-1");
        assertThat(result, nullValue());
        // Removing a non referenced data stream name, signal noop, by returning the same instance
        alias = new DataStreamAlias("my-alias", List.of("ds-1"), null);
        result = alias.removeDataStream("ds-2");
        assertThat(result, sameInstance(alias));
    }

    public void testIntersect() {
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), null);
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), null);
            DataStreamAlias result = alias1.intersect(s -> alias2.getDataStreams().contains(s));
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-2"));
            assertThat(result.getWriteDataStream(), nullValue());
        }
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), "ds-2");
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), null);
            DataStreamAlias result = alias1.intersect(s -> alias2.getDataStreams().contains(s));
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-2"));
            assertThat(result.getWriteDataStream(), equalTo("ds-2"));
        }
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), null);
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), "ds-3");
            DataStreamAlias result = alias1.intersect(s -> alias2.getDataStreams().contains(s));
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-2"));
            assertThat(result.getWriteDataStream(), nullValue());
        }
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2", "ds-3"), "ds-3");
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), "ds-2");
            DataStreamAlias result = alias1.intersect(s -> alias2.getDataStreams().contains(s));
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-2", "ds-3"));
            assertThat(result.getWriteDataStream(), equalTo("ds-3"));
        }
    }

    public void testMerge() {
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), null);
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), null);
            DataStreamAlias result = alias1.merge(alias2);
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-2", "ds-3"));
            assertThat(result.getWriteDataStream(), nullValue());
        }
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), "ds-2");
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), null);
            DataStreamAlias result = alias1.merge(alias2);
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-2", "ds-3"));
            assertThat(result.getWriteDataStream(), equalTo("ds-2"));
        }
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), "ds-2");
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), "ds-3");
            DataStreamAlias result = alias1.merge(alias2);
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-2", "ds-3"));
            assertThat(result.getWriteDataStream(), equalTo("ds-2"));
        }
        {
            DataStreamAlias alias1 = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), null);
            DataStreamAlias alias2 = new DataStreamAlias("my-alias", List.of("ds-2", "ds-3"), "ds-3");
            DataStreamAlias result = alias1.merge(alias2);
            assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-2", "ds-3"));
            assertThat(result.getWriteDataStream(), equalTo("ds-3"));
        }
    }

    public void testRenameDataStreams() {
        DataStreamAlias alias = new DataStreamAlias("my-alias", List.of("ds-1", "ds-2"), "ds-2");
        DataStreamAlias result = alias.renameDataStreams("ds-2", "ds-3");
        assertThat(result.getDataStreams(), containsInAnyOrder("ds-1", "ds-3"));
        assertThat(result.getWriteDataStream(), equalTo("ds-3"));
    }
}
