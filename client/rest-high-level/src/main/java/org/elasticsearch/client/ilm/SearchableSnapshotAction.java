/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link LifecycleAction} that will convert the index into a searchable snapshot.
 */
public class SearchableSnapshotAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "searchable_snapshot";

    public static final ParseField SNAPSHOT_REPOSITORY = new ParseField("snapshot_repository");
    public static final ParseField FORCE_MERGE_INDEX = new ParseField("force_merge_index");

    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new SearchableSnapshotAction((String) a[0], a[1] == null || (boolean) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_REPOSITORY);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FORCE_MERGE_INDEX);
    }

    public static SearchableSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String snapshotRepository;
    private final boolean forceMergeIndex;

    public SearchableSnapshotAction(String snapshotRepository, boolean forceMergeIndex) {
        if (Strings.hasText(snapshotRepository) == false) {
            throw new IllegalArgumentException("the snapshot repository must be specified");
        }
        this.snapshotRepository = snapshotRepository;
        this.forceMergeIndex = forceMergeIndex;
    }

    public SearchableSnapshotAction(String snapshotRepository) {
        this(snapshotRepository, true);
    }

    boolean isForceMergeIndex() {
        return forceMergeIndex;
    }

    public String getSnapshotRepository() {
        return snapshotRepository;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT_REPOSITORY.getPreferredName(), snapshotRepository);
        builder.field(FORCE_MERGE_INDEX.getPreferredName(), forceMergeIndex);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchableSnapshotAction that = (SearchableSnapshotAction) o;
        return forceMergeIndex == that.forceMergeIndex && snapshotRepository.equals(that.snapshotRepository);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotRepository, forceMergeIndex);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
