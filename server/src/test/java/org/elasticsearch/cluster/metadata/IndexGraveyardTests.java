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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Tests for the {@link IndexGraveyard} class
 */
public class IndexGraveyardTests extends ESTestCase {

    public void testEquals() {
        final IndexGraveyard graveyard = createRandom();
        assertThat(graveyard, equalTo(IndexGraveyard.builder(graveyard).build()));
        final IndexGraveyard.Builder newGraveyard = IndexGraveyard.builder(graveyard);
        newGraveyard.addTombstone(new Index(randomAlphaOfLengthBetween(4, 15), UUIDs.randomBase64UUID()));
        assertThat(newGraveyard.build(), not(graveyard));
    }

    public void testSerialization() throws IOException {
        final IndexGraveyard graveyard = createRandom();
        final BytesStreamOutput out = new BytesStreamOutput();
        graveyard.writeTo(out);
        assertThat(new IndexGraveyard(out.bytes().streamInput()), equalTo(graveyard));
    }

    public void testXContent() throws IOException {
        final IndexGraveyard graveyard = createRandom();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        graveyard.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        if (graveyard.getTombstones().size() > 0) {
            // check that date properly printed
            assertThat(Strings.toString(graveyard, false, true),
                containsString(XContentElasticsearchExtension.DEFAULT_DATE_PRINTER
                        .print(graveyard.getTombstones().get(0).getDeleteDateInMillis())));
        }
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken(); // the beginning of the parser
        assertThat(IndexGraveyard.fromXContent(parser), equalTo(graveyard));
    }

    public void testAddTombstones() {
        final IndexGraveyard graveyard1 = createRandom();
        final IndexGraveyard.Builder graveyardBuidler = IndexGraveyard.builder(graveyard1);
        final int numAdds = randomIntBetween(0, 4);
        for (int j = 0; j < numAdds; j++) {
            graveyardBuidler.addTombstone(new Index("nidx-" + j, UUIDs.randomBase64UUID()));
        }
        final IndexGraveyard graveyard2 = graveyardBuidler.build();
        if (numAdds == 0) {
            assertThat(graveyard2, equalTo(graveyard1));
        } else {
            assertThat(graveyard2, not(graveyard1));
            assertThat(graveyard1.getTombstones().size(), lessThan(graveyard2.getTombstones().size()));
            assertThat(Collections.indexOfSubList(graveyard2.getTombstones(), graveyard1.getTombstones()), equalTo(0));
        }
    }

    public void testPurge() {
        // try with max tombstones as some positive integer
        executePurgeTestWithMaxTombstones(randomIntBetween(1, 20));
        // try with max tombstones as the default
        executePurgeTestWithMaxTombstones(IndexGraveyard.SETTING_MAX_TOMBSTONES.getDefault(Settings.EMPTY));
    }

    public void testDiffs() {
        IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder();
        final int numToPurge = randomIntBetween(0, 4);
        final List<Index> removals = new ArrayList<>();
        for (int i = 0; i < numToPurge; i++) {
            final Index indexToRemove = new Index("ridx-" + i, UUIDs.randomBase64UUID());
            graveyardBuilder.addTombstone(indexToRemove);
            removals.add(indexToRemove);
        }
        final int numTombstones = randomIntBetween(0, 4);
        for (int i = 0; i < numTombstones; i++) {
            graveyardBuilder.addTombstone(new Index("idx-" + i, UUIDs.randomBase64UUID()));
        }
        final IndexGraveyard graveyard1 = graveyardBuilder.build();
        graveyardBuilder = IndexGraveyard.builder(graveyard1);
        final int numToAdd = randomIntBetween(0, 4);
        final List<Index> additions = new ArrayList<>();
        for (int i = 0; i < numToAdd; i++) {
            final Index indexToAdd = new Index("nidx-" + i, UUIDs.randomBase64UUID());
            graveyardBuilder.addTombstone(indexToAdd);
            additions.add(indexToAdd);
        }
        final IndexGraveyard graveyard2 = graveyardBuilder.build(settingsWithMaxTombstones(numTombstones + numToAdd));
        final int numPurged = graveyardBuilder.getNumPurged();
        assertThat(numPurged, equalTo(numToPurge));
        final IndexGraveyard.IndexGraveyardDiff diff = new IndexGraveyard.IndexGraveyardDiff(graveyard1, graveyard2);
        final List<Index> actualAdded = diff.getAdded().stream().map(t -> t.getIndex()).collect(Collectors.toList());
        assertThat(new HashSet<>(actualAdded), equalTo(new HashSet<>(additions)));
        assertThat(diff.getRemovedCount(), equalTo(removals.size()));
    }

    public void testContains() {
        List<Index> indices = new ArrayList<>();
        final int numIndices = randomIntBetween(1, 5);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new Index("idx-" + i, UUIDs.randomBase64UUID()));
        }
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (final Index index : indices) {
            graveyard.addTombstone(index);
        }
        final IndexGraveyard indexGraveyard = graveyard.build();
        for (final Index index : indices) {
            assertTrue(indexGraveyard.containsIndex(index));
        }
        assertFalse(indexGraveyard.containsIndex(new Index(randomAlphaOfLength(6), UUIDs.randomBase64UUID())));
    }

    public static IndexGraveyard createRandom() {
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        final int numTombstones = randomIntBetween(0, 4);
        for (int i = 0; i < numTombstones; i++) {
            graveyard.addTombstone(new Index("idx-" + i, UUIDs.randomBase64UUID()));
        }
        return graveyard.build();
    }

    private void executePurgeTestWithMaxTombstones(final int maxTombstones) {
        final int numExtra = randomIntBetween(1, 10);
        final IndexGraveyard.Builder graveyardBuilder = createWithDeletions(maxTombstones + numExtra);
        final IndexGraveyard graveyard = graveyardBuilder.build(settingsWithMaxTombstones(maxTombstones));
        final int numPurged = graveyardBuilder.getNumPurged();
        assertThat(numPurged, equalTo(numExtra));
        assertThat(graveyard.getTombstones().size(), equalTo(maxTombstones));
    }

    private static IndexGraveyard.Builder createWithDeletions(final int numAdd) {
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (int i = 0; i < numAdd; i++) {
            graveyard.addTombstone(new Index("idx-" + i, UUIDs.randomBase64UUID()));
        }
        return graveyard;
    }

    private static Settings settingsWithMaxTombstones(final int maxTombstones) {
        return Settings.builder().put(IndexGraveyard.SETTING_MAX_TOMBSTONES.getKey(), maxTombstones).build();
    }

}
