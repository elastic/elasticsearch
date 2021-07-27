/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.compress.LZ4;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Builds the time series id ({@code _tsid}) from a {@code _source}, a
 * {@link byte[]} that uniquely identifies the time series to which that
 * {@code _source} belongs. Put another way: if you think of the
 * {@code _source} as a document describing the state of some real
 * world <strong>thing</strong> then the {@code _tsid} uniquely
 * identifies that thing.
 * <h2>The Story</h2>
 * <p>
 * Uniquely identifying a thing is useful if, say, that thing exposes
 * an ever increasing counter and you want to calculate its rate of change.
 * The ur-example of this sort of thing is routers and the ur-implementation
 * of the rate of change is <a href="https://oss.oetiker.ch/rrdtool/">RRDTool</a>.
 * More modern examples are things like
 * <a href="https://prometheus.io/">Prometheus</a>. These are wonderful tools
 * with good ideas.
 * <p>
 * Those systems born from time series data typically group the metrics by their
 * unique time series identifier, sorted by time. Like, that's the on disk
 * structure. For RRDTool it is literally a file named for the time series
 * containing a couple of circular buffers for measurements. The index into the
 * buffer is pretty much {@code (time - start_offset)/resolution}.
 * <p>
 * Elasticsearch, being a search engine at heart, doesn't organize things that
 * way. All of our "time based" data, including metric data, is organized in
 * indices that roughly correspond to a time range. This is useful because we
 * build many write-once files (segments) to power our search engine. Deleting
 * documents in an index is expensive so we prefer you delete the entire index
 * when it gets too old. Logan's Run for data.
 * <p>
 * Those roughly time ranged indices are further sharded based on a routing key,
 * usually a document's randomly generated id. But for time series data we really
 * want to group documents corresponding to the same real world thing together.
 * So we route on {@code _tsid} instead. Further, we sort sort each segment on
 * {@code _tsid, @timestamp} to put the documents <strong>right</strong> next to
 * each other. This allows even simple compression algorithsm like {@link LZ4}
 * to do a very good job compressing the {@code _source}.
 * <p>
 * Let's take a quite detour to talk about why this compression is important. We
 * tend to store much more data than the {@code _tsid} and the measurement
 * in each document. In documents about k8s containers, for example, we'll store
 * the container's id (the {@code _tsid}) and the memory usage (a measurement) but
 * we'll also store the url of the image used to build the pod, the pod's name, the
 * name of the node it's running on, the name of the agent sampling the data, the
 * version of the agent sampling the data, etc. All of that "extra" is useful and
 * we should get the best compression out of it we can.
 * <h2>The Constraints</h2>
 * As much as we'd like to be a wonderful storage system for metrics we really don't
 * want to rewrite any of Elasticsearch's traditional assumptions. Elasticsearch
 * has always stored the original {@code _source} document, for example, and we
 * stick to that proud tradition, working hard to compress it well.
 * <p>
 * Similarly, Elasticsearch has always been ok with documents arriving "out of
 * order". By that, I mean that documents that correspond to a measurement taken
 * at a certain time can get delayed. And that Elasticsearch doesn't mind those
 * delays. It'll accept the document. We'd like to keep this behavior.
 * <p>
 * Elasticsearch has traditionally been quite ok adding new fields to an index
 * on the fly. And we'd like to keep that too. Even for dimension fields. So, if
 * you add a new dimension to an existing index we have to make sure it doesn't
 * get confused and start trying to route documents to the wrong place.
 * <p>
 * The length of the {@code _tsid} doesn't really matter very much. Remember, we're
 * grouping like documents together. So they compress well. It costs bytes over
 * the wire and in memory so it shouldn't be massive, but it doesn't need to be
 * super tiny.
 * <p>
 * When it comes time use the {@code _tsid} to do fun stuff like detect the rate
 * of change of a counter it would screw things up if two "counters" got the same
 * {@code _tsid}.
 * <p>
 * It's pretty useful to be able to "parse" the {@code _tsid} into the values
 * of the dimensions that it encoded. It's useful to be able to do that without
 * any index metadata.
 * <h2>The {@code _tsid}</h2>
 * Given these constraints the {@code _tsid} is <strong>just</strong> the field
 * names and their values plus a little type information.
 * <h2>Where does this happen?!</h2>
 * We generate the {@code _tsid} on the node coordinating the index action so that
 * it can be included in the routing.
 */
public final class TimeSeriesIdGenerator {
    /**
     * The maximum length of the tsid. The value itself comes from a range check in
     * Lucene's writer for utf-8 doc values.
     */
    private static final int LIMIT = ByteBlockPool.BYTE_BLOCK_SIZE - 2;
    /**
     * Maximum length of the name of dimension. We picked this so that we could
     * comfortable fit 16 dimensions inside {@link #LIMIT}.
     */
    private static final int DIMENSION_NAME_LIMIT = 512;
    /**
     * The maximum length of any single dimension. We picked this so that we could
     * comfortable fit 16 dimensions inside {@link #LIMIT}. This should be quite
     * comfortable given that dimensions are typically going to be less than a
     * hundred bytes each, but we're being paranoid here.
     */
    private static final int DIMENSION_VALUE_LIMIT = 1024;

    private final ObjectComponent root;

    public TimeSeriesIdGenerator(ObjectComponent root) {
        if (root == null) {
            /*
             * This can happen if an index is configured in time series mode
             * without any mapping. It's fine - we'll add dimensions later.
             * For now it'll make a generator that will fail to index any
             * documents. Which is totally ok.
             */
            root = new ObjectComponent(Map.of());
        }
        root.collectDimensionNames("", name -> {
            int bytes = UnicodeUtil.calcUTF16toUTF8Length(name, 0, name.length());
            if (bytes > DIMENSION_NAME_LIMIT) {
                throw new IllegalArgumentException(
                    "Dimension name must be less than [" + DIMENSION_NAME_LIMIT + "] bytes but [" + name + "] was [" + bytes + "]"
                );
            }
        });
        this.root = root;
    }

    @Override
    public String toString() {
        return "extract dimensions using " + root;
    }

    /**
     * Build the tsid from the {@code _source}. See class docs for more on what it looks like and why.
     */
    public BytesReference generate(XContentParser parser) throws IOException {
        List<Map.Entry<String, CheckedConsumer<StreamOutput, IOException>>> values = new ArrayList<>();
        parser.nextToken();
        root.extract(values, "", parser);
        if (values.isEmpty()) {
            List<String> dimensionNames = new ArrayList<>();
            root.collectDimensionNames("", dimensionNames::add);
            if (dimensionNames.isEmpty()) {
                throw new IllegalArgumentException("There aren't any mapped dimensions");
            }
            Collections.sort(dimensionNames);
            throw new IllegalArgumentException("Document must contain one of the dimensions " + dimensionNames);
        }
        Collections.sort(values, Comparator.comparing(Map.Entry::getKey));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(values.size());
            for (Map.Entry<String, CheckedConsumer<StreamOutput, IOException>> v : values) {
                out.writeBytesRef(new BytesRef(v.getKey())); // Write in utf-8 instead of writeString's utf-16-ish thing
                v.getValue().accept(out);
            }
            BytesReference bytes = out.bytes();
            if (bytes.length() > LIMIT) {
                throw new IllegalArgumentException("tsid longer than [" + LIMIT + "] bytes [" + bytes.length() + "]");
            }
            return bytes;
        }
    }

    /**
     * Parse the {@code _tsid} into a human readable map.
     */
    public static Map<String, Object> parse(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Object> result = new LinkedHashMap<String, Object>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            try {
                int type = in.read();
                switch (type) {
                    case (byte) 's':
                        result.put(name, in.readBytesRef().utf8ToString());
                        break;
                    case (byte) 'l':
                        result.put(name, in.readLong());
                        break;
                    default:
                        throw new IllegalArgumentException("known type [" + type + "]");
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("can't parse [" + name + "]: " + e.getMessage(), e);
            }
        }
        return result;
    }

    public abstract static class Component {
        private Component() {}

        abstract void extract(
            List<Map.Entry<String, CheckedConsumer<StreamOutput, IOException>>> values,
            String name,
            XContentParser parser
        ) throws IOException;

        abstract void collectDimensionNames(String name, Consumer<String> consumer);
    }

    public static final class ObjectComponent extends Component {
        private final Map<String, Component> components;

        public ObjectComponent(Map<String, Component> components) {
            this.components = components;
            for (Map.Entry<String, Component> c : components.entrySet()) {
                if (c.getValue() == null) {
                    throw new IllegalStateException("null components not supported but [" + c.getKey() + "] was null");
                }
            }
        }

        @Override
        void extract(List<Map.Entry<String, CheckedConsumer<StreamOutput, IOException>>> values, String name, XContentParser parser)
            throws IOException {
            ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser);
            while (parser.nextToken() != Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                Component sub = components.get(fieldName);
                if (sub == null) {
                    parser.skipChildren();
                    continue;
                }
                sub.extract(values, name.isEmpty() ? fieldName : name + "." + fieldName, parser);
            }
        }

        @Override
        void collectDimensionNames(String name, Consumer<String> consumer) {
            for (Map.Entry<String, Component> c : components.entrySet()) {
                c.getValue().collectDimensionNames(name.isEmpty() ? c.getKey() : name + "." + c.getKey(), consumer);
            }
        }

        @Override
        public String toString() {
            return components.toString();
        }
    }

    public abstract static class LeafComponent extends Component {
        private LeafComponent() {}

        protected abstract CheckedConsumer<StreamOutput, IOException> extractLeaf(XContentParser parser) throws IOException;

        @Override
        void extract(List<Map.Entry<String, CheckedConsumer<StreamOutput, IOException>>> values, String name, XContentParser parser)
            throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY || parser.currentToken() == XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Dimensions must be single valued but got [" + parser.currentToken() + "]");
            }
            try {
                values.add(Map.entry(name, extractLeaf(parser)));
            } catch (IllegalArgumentException | IOException e) {
                throw new IllegalArgumentException("error extracting dimension [" + name + "]: " + e.getMessage(), e);
            }
        }

        @Override
        void collectDimensionNames(String name, Consumer<String> dimensionNames) {
            dimensionNames.accept(name);
        }
    }

    public abstract static class StringLeaf extends LeafComponent {
        protected abstract String extractString(XContentParser parser) throws IOException;

        @Override
        protected final CheckedConsumer<StreamOutput, IOException> extractLeaf(XContentParser parser) throws IOException {
            String value = extractString(parser);
            if (value == null) {
                throw new IllegalArgumentException("null values not allowed");
            }
            /*
             * Write in utf8 instead of StreamOutput#writeString which is utf-16-ish
             * so its easier for folks to reason about the space taken up. Mostly
             * it'll be smaller too.
             */
            BytesRef bytes = new BytesRef(value);
            if (bytes.length > DIMENSION_VALUE_LIMIT) {
                throw new IllegalArgumentException("longer than [" + DIMENSION_VALUE_LIMIT + "] bytes [" + bytes.length + "]");
            }
            return out -> {
                out.write((byte) 's');
                out.writeBytesRef(bytes);
            };
        }
    }

    public abstract static class LongLeaf extends LeafComponent {
        protected abstract long extractLong(XContentParser parser) throws IOException;

        @Override
        protected final CheckedConsumer<StreamOutput, IOException> extractLeaf(XContentParser parser) throws IOException {
            long value = extractLong(parser);
            return out -> {
                out.write((byte) 'l');
                out.writeLong(value);
            };
        }
    }
}
