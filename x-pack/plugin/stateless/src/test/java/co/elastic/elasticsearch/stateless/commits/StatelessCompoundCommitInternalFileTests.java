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
                    file("_2_1_Lucene90_0.dvd", 69),
                    file("_1_1_Lucene90_0.dvm", 75),
                    file("_1_2_Lucene90_0.dvd", 76),
                    file("_0_2_Lucene90_0.dvd", 88),
                    file("_2_1.fnm", 88),
                    file("_0_1_Lucene90_0.dvd", 89),
                    file("_0_1_Lucene90_0.dvm", 94),
                    file("_1_2_Lucene90_0.dvm", 99),
                    file("_1_2.fnm", 102),
                    file("_2_1_Lucene90_0.dvm", 102),
                    file("_1_1_Lucene90_0.dvd", 107),
                    file("_0_2_Lucene90_0.dvm", 111),
                    file("_0_2.fnm", 115),
                    file("_0_1.fnm", 148),
                    file("_1_1.fnm", 169),
                    file("_1.cfs", 199),
                    file("_1.si", 205),
                    file("_1.cfe", 264),
                    file("_2.cfs", 265),
                    file("_2.cfe", 293),
                    file("_2.si", 301),
                    file("segments_5", 1254),
                    file("segments_6", 1542)
                )
            )
        );
    }

    public void testMultipleCFESort() {
        var unsortedInternalFiles = shuffledList(
            List.of(
                file("segments_aw2y", 1037),
                file("_41mks.si", 398),
                file("_41ml3.si", 399),
                file("_41ml4.si", 361),
                file("_41ml5.si", 361),
                file("_41ml6.si", 361),
                file("_41ml7.si", 361),
                file("_41ml3_1.fnm", 4193),
                file("_41ml4_1.fnm", 3865),
                file("_41mks_2.fnm", 7174),
                file("_41ml3_1_Lucene90_0.dvd", 93),
                file("_41ml3_1_Lucene90_0.dvm", 160),
                file("_41ml4_1_Lucene90_0.dvd", 75),
                file("_41ml4_1_Lucene90_0.dvm", 160),
                file("_41mks_2_Lucene90_0.dvd", 293),
                file("_41mks_2_Lucene90_0.dvm", 160),
                file("_41mks.cfe", 697),
                file("_41mks.cfs", 78713),
                file("_41ml3.cfe", 595),
                file("_41ml3.cfs", 19785),
                file("_41ml4.cfe", 595),
                file("_41ml4.cfs", 11343),
                file("_41ml5.cfe", 595),
                file("_41ml5.cfs", 5008),
                file("_41ml6.cfe", 595),
                file("_41ml6.cfs", 11343),
                file("_41ml7.cfe", 595),
                file("_41ml7.cfs", 5008)
            )
        );

        assertThat(
            unsortedInternalFiles.stream().sorted().toList(),
            equalTo(
                List.of(
                    file("_41ml4_1_Lucene90_0.dvd", 75),
                    file("_41ml3_1_Lucene90_0.dvd", 93),
                    file("_41mks_2_Lucene90_0.dvm", 160),
                    file("_41ml3_1_Lucene90_0.dvm", 160),
                    file("_41ml4_1_Lucene90_0.dvm", 160),
                    file("_41mks_2_Lucene90_0.dvd", 293),
                    file("_41ml4.si", 361),
                    file("_41ml5.si", 361),
                    file("_41ml6.si", 361),
                    file("_41ml7.si", 361),
                    file("_41mks.si", 398),
                    file("_41ml3.si", 399),
                    file("_41ml3.cfe", 595),
                    file("_41ml4.cfe", 595),
                    file("_41ml5.cfe", 595),
                    file("_41ml6.cfe", 595),
                    file("_41ml7.cfe", 595),
                    file("_41mks.cfe", 697),
                    file("segments_aw2y", 1037),
                    file("_41ml4_1.fnm", 3865),
                    file("_41ml3_1.fnm", 4193),
                    file("_41ml5.cfs", 5008),
                    file("_41ml7.cfs", 5008),
                    file("_41mks_2.fnm", 7174),
                    file("_41ml4.cfs", 11343),
                    file("_41ml6.cfs", 11343),
                    file("_41ml3.cfs", 19785),
                    file("_41mks.cfs", 78713)
                )
            )
        );
    }

    public void testCFEAndRegularSegmentsSort() {
        var unsortedInternalFiles = shuffledList(
            List.of(
                file("segments_6n9", 5206),
                file("_rks.si", 584),
                file("_rsm.si", 361),
                file("_rsn.si", 361),
                file("_rso.si", 361),
                file("_rsp.si", 361),
                file("_rks.fnm", 9401),
                file("_rsm.cfe", 425),
                file("_rsm.cfs", 705461),
                file("_rsn.cfe", 425),
                file("_rsn.cfs", 144514),
                file("_rso.cfe", 425),
                file("_rso.cfs", 3964123),
                file("_rsp.cfe", 425),
                file("_rsp.cfs", 1657860),
                file("_rks_ES812Postings_0.doc", 472857214),
                file("_rks_ES812Postings_0.tim", 40133762),
                file("_rks_ES812Postings_0.tip", 2559177),
                file("_rks_ES812Postings_0.tmd", 3739),
                file("_rks.fdm", 791),
                file("_rks.fdt", 463032785),
                file("_rks.fdx", 59089),
                file("_rks.kdd", 69843813),
                file("_rks.kdi", 307265),
                file("_rks.kdm", 548),
                file("_rks_Lucene90_0.dvd", 647851632),
                file("_rks_Lucene90_0.dvm", 14738)
            )
        );

        assertThat(
            unsortedInternalFiles.stream().sorted().toList(),
            equalTo(
                List.of(
                    file("_rsm.si", 361),
                    file("_rsn.si", 361),
                    file("_rso.si", 361),
                    file("_rsp.si", 361),
                    file("_rsm.cfe", 425),
                    file("_rsn.cfe", 425),
                    file("_rso.cfe", 425),
                    file("_rsp.cfe", 425),
                    file("_rks.kdm", 548),
                    file("_rks.si", 584),
                    file("_rks.fdm", 791),
                    file("_rks_ES812Postings_0.tmd", 3739),
                    file("segments_6n9", 5206),
                    file("_rks.fnm", 9401),
                    file("_rks_Lucene90_0.dvm", 14738),
                    file("_rks.fdx", 59089),
                    file("_rsn.cfs", 144514),
                    file("_rks.kdi", 307265),
                    file("_rsm.cfs", 705461),
                    file("_rsp.cfs", 1657860),
                    file("_rks_ES812Postings_0.tip", 2559177),
                    file("_rso.cfs", 3964123),
                    file("_rks_ES812Postings_0.tim", 40133762),
                    file("_rks.kdd", 69843813),
                    file("_rks.fdt", 463032785),
                    file("_rks_ES812Postings_0.doc", 472857214),
                    file("_rks_Lucene90_0.dvd", 647851632)
                )
            )
        );

    }

    private static InternalFile file(String name, int length) {
        return new InternalFile(name, length);
    }
}
