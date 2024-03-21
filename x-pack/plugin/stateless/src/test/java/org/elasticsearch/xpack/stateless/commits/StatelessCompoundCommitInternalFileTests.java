/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */
package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.InternalFile;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

public class StatelessCompoundCommitInternalFileTests extends ESTestCase {

    public void testSortInternalFiles() {
        var internalFiles = shuffledList(
            List.of(
                file("segments_5", 1254),
                file("_1.si", 205),
                file("_0_1.fnm", 148),
                file("_1_1.fnm", 169),
                file("_0_1_Lucene90_0.dvd", 89),
                file("_0_1_Lucene90_0.dvm", 94),
                file("_1_1_Lucene90_0.dvd", 107),
                file("_1_1_Lucene90_0.dvm", 75),
                file("_1.cfe", 264),
                file("_1.cfs", 199),
                file("segments_6", 1542),
                file("_2.si", 301),
                file("_0_2.fnm", 115),
                file("_1_2.fnm", 102),
                file("_2_1.fnm", 88),
                file("_2_1_Lucene90_0.dvd", 69),
                file("_2_1_Lucene90_0.dvm", 102),
                file("_0_2_Lucene90_0.dvd", 88),
                file("_0_2_Lucene90_0.dvm", 111),
                file("_1_2_Lucene90_0.dvd", 76),
                file("_1_2_Lucene90_0.dvm", 99),
                file("_2.cfe", 293),
                file("_2.cfs", 265)
            )
        );
        assertThat(
            internalFiles.stream().sorted().toList(),
            equalTo(
                List.of(
                    file("segments_5", 1254),
                    file("segments_6", 1542),
                    file("_1.si", 205),
                    file("_2.si", 301),
                    file("_0_1.fnm", 148),
                    file("_1_1.fnm", 169),
                    file("_2_1.fnm", 88),
                    file("_0_2.fnm", 115),
                    file("_1_2.fnm", 102),
                    file("_0_1_Lucene90_0.dvd", 89),
                    file("_0_1_Lucene90_0.dvm", 94),
                    file("_1_1_Lucene90_0.dvd", 107),
                    file("_1_1_Lucene90_0.dvm", 75),
                    file("_2_1_Lucene90_0.dvd", 69),
                    file("_2_1_Lucene90_0.dvm", 102),
                    file("_0_2_Lucene90_0.dvd", 88),
                    file("_0_2_Lucene90_0.dvm", 111),
                    file("_1_2_Lucene90_0.dvd", 76),
                    file("_1_2_Lucene90_0.dvm", 99),
                    file("_1.cfe", 264),
                    file("_1.cfs", 199),
                    file("_2.cfe", 293),
                    file("_2.cfs", 265)
                )
            )
        );
    }

    private static InternalFile file(String name, int length) {
        return new InternalFile(name, length);
    }
}
