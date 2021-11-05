/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchableSnapshotsUtilsTests extends ESTestCase {

    public void testSegmentFilenameComparator() {

        List<Tuple<Long, String>> fileNames = new ArrayList<>();
        while (fileNames.size() < 100) {
            final long generation = randomLongBetween(1, 10000);
            fileNames.add(Tuple.tuple(generation, IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", generation)));
        }

        fileNames.sort(Comparator.comparing(Tuple::v2, SearchableSnapshotsUtils.SEGMENT_FILENAME_COMPARATOR));
        long previousGeneration = 0;
        for (Tuple<Long, String> fileName : fileNames) {
            assertThat(
                fileNames + " should be sorted, found disorder at " + fileName,
                fileName.v1(),
                greaterThanOrEqualTo(previousGeneration)
            );
            previousGeneration = fileName.v1();
        }

    }

}
