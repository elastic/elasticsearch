/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link LifecycleAction} that will convert the index into a searchable snapshot.
 */
public class SearchableSnapshotAction implements LifecycleAction, ToXContentObject {
    public static final String NAME = "searchable_snapshot";

    public static final ParseField SNAPSHOT_REPOSITORY = new ParseField("snapshot_repository");
    public static final ParseField FORCE_MERGE_INDEX = new ParseField("force_merge_index");


    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        true, a -> new SearchableSnapshotAction((String) a[0], a[1] == null || (boolean) a[1]));

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
        return forceMergeIndex == that.forceMergeIndex &&
            snapshotRepository.equals(that.snapshotRepository);
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
