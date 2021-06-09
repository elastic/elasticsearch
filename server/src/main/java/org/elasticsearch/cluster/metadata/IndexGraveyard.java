/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * A collection of tombstones for explicitly marking indices as deleted in the cluster state.
 *
 * The cluster state contains a list of index tombstones for indices that have been
 * deleted in the cluster.  Because cluster states are processed asynchronously by
 * nodes and a node could be removed from the cluster for a period of time, the
 * tombstones remain in the cluster state for a fixed period of time, after which
 * they are purged.
 */
public final class IndexGraveyard implements Metadata.Custom {

    /**
     * Setting for the maximum tombstones allowed in the cluster state;
     * prevents the cluster state size from exploding too large, but it opens the
     * very unlikely risk that if there are greater than MAX_TOMBSTONES index
     * deletions while a node was offline, when it comes back online, it will have
     * missed index deletions that it may need to process.
     */
    public static final Setting<Integer> SETTING_MAX_TOMBSTONES = Setting.intSetting("cluster.indices.tombstones.size",
                                                                                     500, // the default maximum number of tombstones
                                                                                     Setting.Property.NodeScope);

    public static final String TYPE = "index-graveyard";
    private static final ParseField TOMBSTONES_FIELD = new ParseField("tombstones");
    private static final ObjectParser<List<Tombstone>, Void> GRAVEYARD_PARSER;
    static {
        GRAVEYARD_PARSER = new ObjectParser<>("index_graveyard", ArrayList::new);
        GRAVEYARD_PARSER.declareObjectArray(List::addAll, Tombstone.getParser(), TOMBSTONES_FIELD);
    }

    private final List<Tombstone> tombstones;

    private IndexGraveyard(final List<Tombstone> list) {
        assert list != null;
        tombstones = Collections.unmodifiableList(list);
    }

    public IndexGraveyard(final StreamInput in) throws IOException {
        this.tombstones = Collections.unmodifiableList(in.readList(Tombstone::new));
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof IndexGraveyard) && Objects.equals(tombstones, ((IndexGraveyard)obj).tombstones);
    }

    @Override
    public int hashCode() {
        return tombstones.hashCode();
    }

    /**
     * Get the current unmodifiable index tombstone list.
     */
    public List<Tombstone> getTombstones() {
        return tombstones;
    }

    /**
     * Returns true if the graveyard contains a tombstone for the given index.
     */
    public boolean containsIndex(final Index index) {
        for (Tombstone tombstone : tombstones) {
            if (tombstone.getIndex().equals(index)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startArray(TOMBSTONES_FIELD.getPreferredName());
        for (Tombstone tombstone : tombstones) {
            tombstone.toXContent(builder, params);
        }
        return builder.endArray();
    }

    public static IndexGraveyard fromXContent(final XContentParser parser) throws IOException {
        return new IndexGraveyard(GRAVEYARD_PARSER.parse(parser, null));
    }

    @Override
    public String toString() {
        return "IndexGraveyard[" + tombstones + "]";
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeList(tombstones);
    }

    @Override
    public Diff<Metadata.Custom> diff(final Metadata.Custom previous) {
        return new IndexGraveyardDiff((IndexGraveyard) previous, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(final StreamInput in) throws IOException {
        return new IndexGraveyardDiff(in);
    }

    public static IndexGraveyard.Builder builder() {
        return new IndexGraveyard.Builder();
    }

    public static IndexGraveyard.Builder builder(final IndexGraveyard graveyard) {
        return new IndexGraveyard.Builder(graveyard);
    }

    /**
     * A class to build an IndexGraveyard.
     */
    public static final class Builder {
        private List<Tombstone> tombstones;
        private int numPurged = -1;
        private final long currentTime = System.currentTimeMillis();

        private Builder() {
            tombstones = new ArrayList<>();
        }

        private Builder(IndexGraveyard that) {
            tombstones = new ArrayList<>(that.getTombstones());
        }

        /**
         * A copy of the current tombstones in the builder.
         */
        public List<Tombstone> tombstones() {
            return Collections.unmodifiableList(tombstones);
        }

        /**
         * Add a deleted index to the list of tombstones in the cluster state.
         */
        public Builder addTombstone(final Index index) {
            tombstones.add(new Tombstone(index, currentTime));
            return this;
        }

        /**
         * Add a set of deleted indexes to the list of tombstones in the cluster state.
         */
        public Builder addTombstones(final Collection<Index> indices) {
            for (Index index : indices) {
                addTombstone(index);
            }
            return this;
        }

        /**
         * Add a list of tombstones to the graveyard.
         */
        Builder addBuiltTombstones(final List<Tombstone> tombstones) {
            this.tombstones.addAll(tombstones);
            return this;
        }

        /**
         * Get the number of tombstones that were purged.  This should *only* be called
         * after build() has been called.
         */
        public int getNumPurged() {
            assert numPurged != -1;
            return numPurged;
        }

        /**
         * Purge tombstone entries.  Returns the number of entries that were purged.
         *
         * Tombstones are purged if the number of tombstones in the list
         * is greater than the input parameter of maximum allowed tombstones.
         * Tombstones are purged until the list is equal to the maximum allowed.
         */
        private int purge(final int maxTombstones) {
            int count = tombstones().size() - maxTombstones;
            if (count <= 0) {
                return 0;
            }
            tombstones = tombstones.subList(count, tombstones.size());
            return count;
        }

        public IndexGraveyard build() {
            return build(Settings.EMPTY);
        }

        public IndexGraveyard build(final Settings settings) {
            // first, purge the necessary amount of entries
            numPurged = purge(SETTING_MAX_TOMBSTONES.get(settings));
            return new IndexGraveyard(tombstones);
        }
    }

    /**
     * A class representing a diff of two IndexGraveyard objects.
     */
    public static final class IndexGraveyardDiff implements NamedDiff<Metadata.Custom> {

        private final List<Tombstone> added;
        private final int removedCount;

        IndexGraveyardDiff(final StreamInput in) throws IOException {
            added = Collections.unmodifiableList(in.readList((streamInput) -> new Tombstone(streamInput)));
            removedCount = in.readVInt();
        }

        IndexGraveyardDiff(final IndexGraveyard previous, final IndexGraveyard current) {
            final List<Tombstone> previousTombstones = previous.tombstones;
            final List<Tombstone> currentTombstones = current.tombstones;
            final List<Tombstone> added;
            final int removed;
            if (previousTombstones.isEmpty()) {
                // nothing will have been removed, and all entries in current are new
                added = new ArrayList<>(currentTombstones);
                removed = 0;
            } else if (currentTombstones.isEmpty()) {
                // nothing will have been added, and all entries in previous are removed
                added = Collections.emptyList();
                removed = previousTombstones.size();
            } else {
                // look through the back, starting from the end, for added tombstones
                final Tombstone lastAddedTombstone = previousTombstones.get(previousTombstones.size() - 1);
                final int addedIndex = currentTombstones.lastIndexOf(lastAddedTombstone);
                if (addedIndex < currentTombstones.size()) {
                    added = currentTombstones.subList(addedIndex + 1, currentTombstones.size());
                } else {
                    added = Collections.emptyList();
                }
                // look from the front for the removed tombstones
                final Tombstone firstTombstone = currentTombstones.get(0);
                int idx = previousTombstones.indexOf(firstTombstone);
                if (idx < 0) {
                    // the first tombstone in the current list wasn't found in the previous list,
                    // which means all tombstones from the previous list have been deleted.
                    assert added.equals(currentTombstones); // all previous are removed, so the current list must be the same as the added
                    idx = previousTombstones.size();
                }
                removed = idx;
            }
            this.added = Collections.unmodifiableList(added);
            this.removedCount = removed;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeList(added);
            out.writeVInt(removedCount);
        }

        @Override
        public IndexGraveyard apply(final Metadata.Custom previous) {
            final IndexGraveyard old = (IndexGraveyard) previous;
            if (removedCount > old.tombstones.size()) {
                throw new IllegalStateException("IndexGraveyardDiff cannot remove [" + removedCount + "] entries from [" +
                                                old.tombstones.size() + "] tombstones.");
            }
            final List<Tombstone> newTombstones = new ArrayList<>(old.tombstones.subList(removedCount, old.tombstones.size()));
            for (Tombstone tombstone : added) {
                newTombstones.add(tombstone);
            }
            return new IndexGraveyard.Builder().addBuiltTombstones(newTombstones).build();
        }

        /** The index tombstones that were added between two states */
        public List<Tombstone> getAdded() {
            return added;
        }

        /** The number of index tombstones that were removed between two states */
        public int getRemovedCount() {
            return removedCount;
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    /**
     * An individual tombstone entry for representing a deleted index.
     */
    public static final class Tombstone implements ToXContentObject, Writeable {

        private static final String INDEX_KEY = "index";
        private static final String DELETE_DATE_IN_MILLIS_KEY = "delete_date_in_millis";
        private static final String DELETE_DATE_KEY = "delete_date";
        private static final ObjectParser<Tombstone.Builder, Void> TOMBSTONE_PARSER;
        static {
            TOMBSTONE_PARSER = new ObjectParser<>("tombstoneEntry", Tombstone.Builder::new);
            TOMBSTONE_PARSER.declareObject(Tombstone.Builder::index, (parser, context) -> Index.fromXContent(parser),
                    new ParseField(INDEX_KEY));
            TOMBSTONE_PARSER.declareLong(Tombstone.Builder::deleteDateInMillis, new ParseField(DELETE_DATE_IN_MILLIS_KEY));
            TOMBSTONE_PARSER.declareString((b, s) -> {}, new ParseField(DELETE_DATE_KEY));
        }

        static final DateFormatter FORMATTER = DateFormatter.forPattern("strict_date_optional_time").withZone(ZoneOffset.UTC);

        static ContextParser<Void, Tombstone> getParser() {
            return (parser, context) -> TOMBSTONE_PARSER.apply(parser, null).build();
        }

        private final Index index;
        private final long deleteDateInMillis;

        private Tombstone(final Index index, final long deleteDateInMillis) {
            Objects.requireNonNull(index);
            if (deleteDateInMillis < 0L) {
                throw new IllegalArgumentException("invalid deleteDateInMillis [" + deleteDateInMillis + "]");
            }
            this.index = index;
            this.deleteDateInMillis = deleteDateInMillis;
        }

        // create from stream
        private Tombstone(StreamInput in) throws IOException {
            index = new Index(in);
            deleteDateInMillis = in.readLong();
        }

        /**
         * The deleted index.
         */
        public Index getIndex() {
            return index;
        }

        /**
         * The date in milliseconds that the index deletion event occurred, used for logging/debugging.
         */
        public long getDeleteDateInMillis() {
            return deleteDateInMillis;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            index.writeTo(out);
            out.writeLong(deleteDateInMillis);
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Tombstone that = (Tombstone) other;
            return index.equals(that.index) && deleteDateInMillis == that.deleteDateInMillis;
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + Long.hashCode(deleteDateInMillis);
            return result;
        }

        @Override
        public String toString() {
            String date = FORMATTER.format(Instant.ofEpochMilli(deleteDateInMillis));
            return "[index=" + index + ", deleteDate=" + date + "]";
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_KEY);
            index.toXContent(builder, params);
            builder.timeField(DELETE_DATE_IN_MILLIS_KEY, DELETE_DATE_KEY, deleteDateInMillis);
            return builder.endObject();
        }

        /**
         * A builder for building tombstone entries.
         */
        private static final class Builder {
            private Index index;
            private long deleteDateInMillis = -1L;

            public void index(final Index index) {
                this.index = index;
            }

            public void deleteDateInMillis(final long deleteDate) {
                this.deleteDateInMillis = deleteDate;
            }

            public Tombstone build() {
                assert index != null;
                assert deleteDateInMillis > -1L;
                return new Tombstone(index, deleteDateInMillis);
            }
        }
    }

}
