/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Checkpoint document to store the checkpoint of a transform
 *
 * The fields:
 *
 *  timestamp the timestamp when this document has been created
 *  checkpoint the checkpoint number, incremented for every checkpoint, if -1 this is a non persisted checkpoint
 *  indices a map of the indices from the source including all checkpoints of all indices matching the source pattern, shard level
 *  time_upper_bound for time-based indices this holds the upper time boundary of this checkpoint
 *
 */
public class TransformCheckpoint implements Writeable, ToXContentObject {

    public static final String EMPTY_NAME = "_empty";
    public static final TransformCheckpoint EMPTY = createEmpty(0);

    public static TransformCheckpoint createEmpty(long timestampMillis) {
        return new TransformCheckpoint(EMPTY_NAME, timestampMillis, -1L, Collections.emptyMap(), timestampMillis);
    }

    // the own checkpoint
    public static final ParseField CHECKPOINT = new ParseField("checkpoint");

    // checkpoint of the indexes (sequence id's)
    public static final ParseField INDICES = new ParseField("indices");

    public static final String NAME = "data_frame_transform_checkpoint";

    private static final ConstructingObjectParser<TransformCheckpoint, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TransformCheckpoint, Void> LENIENT_PARSER = createParser(true);

    private final String transformId;
    private final long timestampMillis;
    private final long checkpoint;
    private final Map<String, long[]> indicesCheckpoints;
    private final long timeUpperBoundMillis;

    private static ConstructingObjectParser<TransformCheckpoint, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TransformCheckpoint, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {
            String id = (String) args[0];
            long timestamp = (Long) args[1];
            long checkpoint = (Long) args[2];

            @SuppressWarnings("unchecked")
            Map<String, long[]> checkpoints = (Map<String, long[]>) args[3];

            Long timeUpperBound = (Long) args[4];

            // ignored, only for internal storage: String docType = (String) args[5];
            return new TransformCheckpoint(id, timestamp, checkpoint, checkpoints, timeUpperBound);
        });

        parser.declareString(constructorArg(), TransformField.ID);

        // note: this is never parsed from the outside where timestamp can be formatted as date time
        parser.declareLong(constructorArg(), TransformField.TIMESTAMP_MILLIS);
        parser.declareLong(constructorArg(), CHECKPOINT);

        parser.declareObject(constructorArg(), (p, c) -> {
            Map<String, long[]> checkPointsByIndexName = new TreeMap<>();
            XContentParser.Token token = null;
            while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(p.getTokenLocation(), "Unexpected token " + token + " ");
                }

                final String indexName = p.currentName();
                token = p.nextToken();
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new ParsingException(p.getTokenLocation(), "Unexpected token " + token + " ");
                }

                long[] checkpoints = p.listOrderedMap().stream().mapToLong(num -> ((Number) num).longValue()).toArray();
                checkPointsByIndexName.put(indexName, checkpoints);
            }
            return checkPointsByIndexName;
        }, INDICES);
        parser.declareLong(optionalConstructorArg(), TransformField.TIME_UPPER_BOUND_MILLIS);
        parser.declareString(optionalConstructorArg(), TransformField.INDEX_DOC_TYPE);

        return parser;
    }

    public TransformCheckpoint(String transformId, long timestamp, long checkpoint, Map<String, long[]> checkpoints, Long timeUpperBound) {
        this.transformId = Objects.requireNonNull(transformId);
        this.timestampMillis = timestamp;
        this.checkpoint = checkpoint;
        this.indicesCheckpoints = Collections.unmodifiableMap(checkpoints);
        this.timeUpperBoundMillis = timeUpperBound == null ? 0 : timeUpperBound;
    }

    public TransformCheckpoint(StreamInput in) throws IOException {
        this.transformId = in.readString();
        this.timestampMillis = in.readLong();
        this.checkpoint = in.readLong();
        this.indicesCheckpoints = readCheckpoints(in.readGenericMap());
        this.timeUpperBoundMillis = in.readLong();
    }

    public boolean isEmpty() {
        return EMPTY_NAME.equals(transformId) && checkpoint == -1;
    }

    /**
     * Whether this checkpoint is a transient (non persisted) checkpoint
     *
     * @return true if this is a transient checkpoint, false otherwise
     */
    public boolean isTransient() {
        return checkpoint == -1;
    }

    /**
     * Create XContent for the purpose of storing it in the internal index
     *
     * Note:
     * @param builder the {@link XContentBuilder}
     * @param params builder specific parameters
     *
     * @return builder instance
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(TransformField.ID.getPreferredName(), transformId);
        builder.field(CHECKPOINT.getPreferredName(), checkpoint);
        builder.field(TransformField.INDEX_DOC_TYPE.getPreferredName(), NAME);
        builder.startObject(INDICES.getPreferredName());
        for (Entry<String, long[]> entry : indicesCheckpoints.entrySet()) {
            builder.array(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.field(TransformField.TIMESTAMP_MILLIS.getPreferredName(), timestampMillis);

        if (timeUpperBoundMillis > 0) {
            builder.field(TransformField.TIME_UPPER_BOUND_MILLIS.getPreferredName(), timeUpperBoundMillis);
        }

        builder.endObject();
        return builder;
    }

    public String getTransformId() {
        return transformId;
    }

    public long getTimestamp() {
        return timestampMillis;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public Map<String, long[]> getIndicesCheckpoints() {
        return indicesCheckpoints;
    }

    public long getTimeUpperBound() {
        return timeUpperBoundMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(transformId);
        out.writeLong(timestampMillis);
        out.writeLong(checkpoint);
        out.writeGenericValue(indicesCheckpoints);
        out.writeLong(timeUpperBoundMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final TransformCheckpoint that = (TransformCheckpoint) other;

        // compare the timestamp, id, checkpoint and than call matches for the rest
        return this.timestampMillis == that.timestampMillis
            && this.checkpoint == that.checkpoint
            && this.timeUpperBoundMillis == that.timeUpperBoundMillis
            && matches(that);
    }

    /**
     * Compares 2 checkpoints ignoring some inner fields.
     *
     * This is for comparing 2 checkpoints to check whether the transform requires an update
     *
     * @param that other checkpoint
     * @return true if checkpoints match
     */
    public boolean matches(TransformCheckpoint that) {
        if (this == that) {
            return true;
        }

        return Objects.equals(this.transformId, that.transformId)
            // quick check
            && this.indicesCheckpoints.size() == that.indicesCheckpoints.size()
            // do the expensive deep equal operation last
            && this.indicesCheckpoints.entrySet()
                .stream()
                .allMatch(e -> Arrays.equals(e.getValue(), that.indicesCheckpoints.get(e.getKey())));
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(transformId, timestampMillis, checkpoint, timeUpperBoundMillis);

        for (Entry<String, long[]> e : indicesCheckpoints.entrySet()) {
            hash = 31 * hash + Objects.hash(e.getKey(), Arrays.hashCode(e.getValue()));
        }
        return hash;
    }

    public static TransformCheckpoint fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static String documentId(String transformId, long checkpoint) {
        if (checkpoint < 0) {
            throw new IllegalArgumentException("checkpoint must be a non-negative number");
        }

        return NAME + "-" + transformId + "-" + checkpoint;
    }

    public static boolean isNullOrEmpty(TransformCheckpoint checkpoint) {
        return checkpoint == null || checkpoint.isEmpty();
    }

    /**
     * Calculate the diff of 2 checkpoints
     *
     * This is to get an indicator for the difference between checkpoints.
     *
     * Note: order is important
     *
     * @param oldCheckpoint the older checkpoint, if transient, newer must be transient, too
     * @param newCheckpoint the newer checkpoint, can be a transient checkpoint
     *
     * @return count number of operations the checkpoint is behind or -1L if it could not calculate the difference
     */
    public static long getBehind(TransformCheckpoint oldCheckpoint, TransformCheckpoint newCheckpoint) {
        if (oldCheckpoint.isTransient()) {
            if (newCheckpoint.isTransient() == false) {
                throw new IllegalArgumentException("can not compare transient against a non transient checkpoint");
            } // else: both are transient
        } else if (newCheckpoint.isTransient() == false && oldCheckpoint.getCheckpoint() > newCheckpoint.getCheckpoint()) {
            throw new IllegalArgumentException("old checkpoint is newer than new checkpoint");
        }

        // get the sum of of shard operations (that are fully replicated), which is 1 higher than the global checkpoint for each shard
        // note: we require shard checkpoints to strictly increase and never decrease
        long oldCheckPointOperationsSum = 0;
        long newCheckPointOperationsSum = 0;

        for (Entry<String, long[]> entry : oldCheckpoint.indicesCheckpoints.entrySet()) {
            // ignore entries that aren't part of newCheckpoint, e.g. deleted indices
            if (newCheckpoint.indicesCheckpoints.containsKey(entry.getKey())) {
                // Add 1 per shard as sequence numbers start at 0, i.e. sequence number 0 means there has been 1 operation
                oldCheckPointOperationsSum += Arrays.stream(entry.getValue()).sum() + entry.getValue().length;
            }
        }

        for (long[] v : newCheckpoint.indicesCheckpoints.values()) {
            // Add 1 per shard as sequence numbers start at 0, i.e. sequence number 0 means there has been 1 operation
            newCheckPointOperationsSum += Arrays.stream(v).sum() + v.length;
        }

        // this should not be possible
        if (newCheckPointOperationsSum < oldCheckPointOperationsSum) {
            return -1L;
        }

        return newCheckPointOperationsSum - oldCheckPointOperationsSum;
    }

    public static Collection<String> getChangedIndices(TransformCheckpoint oldCheckpoint, TransformCheckpoint newCheckpoint) {
        if (oldCheckpoint.isEmpty()) {
            return newCheckpoint.indicesCheckpoints.keySet();
        }

        Set<String> indices = new HashSet<>();

        for (Entry<String, long[]> entry : newCheckpoint.indicesCheckpoints.entrySet()) {
            // compare against the old checkpoint
            if (Arrays.equals(entry.getValue(), oldCheckpoint.indicesCheckpoints.get(entry.getKey())) == false) {
                indices.add(entry.getKey());
            }
        }

        return indices;
    }

    private static Map<String, long[]> readCheckpoints(Map<String, Object> readMap) {
        Map<String, long[]> checkpoints = new TreeMap<>();
        for (Map.Entry<String, Object> e : readMap.entrySet()) {
            if (e.getValue() instanceof long[]) {
                checkpoints.put(e.getKey(), (long[]) e.getValue());
            } else {
                throw new ElasticsearchParseException(
                    "expecting the checkpoints for [{}] to be a long[], but found [{}] instead",
                    e.getKey(),
                    e.getValue().getClass()
                );
            }
        }
        return checkpoints;
    }
}
