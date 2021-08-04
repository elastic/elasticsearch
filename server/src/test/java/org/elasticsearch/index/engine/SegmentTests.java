/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class SegmentTests extends ESTestCase {
    static SortField randomSortField() {
        if (randomBoolean()) {
            SortedNumericSortField field =
                new SortedNumericSortField(randomAlphaOfLengthBetween(1, 10),
                    SortField.Type.INT,
                    randomBoolean(),
                    randomBoolean() ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN);
            if (randomBoolean()) {
                field.setMissingValue(randomInt());
            }
            return field;
        } else {
            SortedSetSortField field =
                new SortedSetSortField(randomAlphaOfLengthBetween(1, 10),
                    randomBoolean(),
                    randomBoolean() ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
            if (randomBoolean()) {
                field.setMissingValue(randomBoolean() ? SortedSetSortField.STRING_FIRST : SortedSetSortField.STRING_LAST);
            }
            return field;
        }
    }

    static Sort randomIndexSort() {
        if (randomBoolean()) {
            return null;
        }
        int size = randomIntBetween(1, 5);
        SortField[] fields = new SortField[size];
        for (int i = 0; i < size; i++) {
            fields[i] = randomSortField();
        }
        return new Sort(fields);
    }

    static Segment randomSegment() {
        Segment segment = new Segment(randomAlphaOfLength(10));
        segment.committed = randomBoolean();
        segment.search = randomBoolean();
        segment.sizeInBytes = randomNonNegativeLong();
        segment.docCount = randomIntBetween(1, Integer.MAX_VALUE);
        segment.delDocCount = randomIntBetween(0, segment.docCount);
        segment.version = Version.LUCENE_8_0_0;
        segment.compound = randomBoolean();
        segment.mergeId = randomAlphaOfLengthBetween(1, 10);
        segment.segmentSort = randomIndexSort();
        if (randomBoolean()) {
            segment.attributes = Collections.singletonMap("foo", "bar");
        }
        return segment;
    }

    public void testSerialization() throws IOException {
        for (int i = 0; i < 20; i++) {
            Segment segment = randomSegment();
            BytesStreamOutput output = new BytesStreamOutput();
            segment.writeTo(output);
            output.flush();
            StreamInput input = output.bytes().streamInput();
            Segment deserialized = new Segment(input);
            assertTrue(isSegmentEquals(deserialized, segment));
        }
    }

    static boolean isSegmentEquals(Segment seg1, Segment seg2) {
        return seg1.docCount == seg2.docCount &&
            seg1.delDocCount == seg2.delDocCount &&
            seg1.committed == seg2.committed &&
            seg1.search == seg2.search &&
            Objects.equals(seg1.version, seg2.version) &&
            Objects.equals(seg1.compound, seg2.compound) &&
            seg1.sizeInBytes == seg2.sizeInBytes &&
            seg1.getGeneration() == seg2.getGeneration() &&
            seg1.getName().equals(seg2.getName()) &&
            seg1.getMergeId().equals(seg2.getMergeId()) &&
            Objects.equals(seg1.segmentSort, seg2.segmentSort);
    }
}
