/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;

import java.util.Collections;

import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesTests extends DesiredNodesTestCase {

    public void testDuplicatedExternalIDsAreNotAllowed() {
        final String duplicatedExternalID = UUIDs.randomBase64UUID();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNodes(
                UUIDs.randomBase64UUID(),
                2,
                randomList(
                    2,
                    10,
                    () -> randomDesiredNode(
                        Version.CURRENT,
                        (settings) -> settings.put(NODE_EXTERNAL_ID_SETTING.getKey(), duplicatedExternalID)
                    )
                )
            )
        );
        assertThat(exception.getMessage(), containsString("Some nodes contain the same setting value"));
    }

    public void testPreviousVersionsWithSameHistoryIDAreSuperseded() {
        final DesiredNodes desiredNodes = new DesiredNodes(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes previousDesiredNodes = new DesiredNodes(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        assertThat(desiredNodes.isSupersededBy(previousDesiredNodes), is(equalTo(false)));
    }

    public void testIsSupersededByADifferentHistoryID() {
        final DesiredNodes desiredNodes = new DesiredNodes(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes differentHistoryID = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        assertThat(desiredNodes.isSupersededBy(differentHistoryID), is(equalTo(true)));
    }

    public void testHasSameVersion() {
        final DesiredNodes desiredNodes = new DesiredNodes(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes desiredNodesWithDifferentVersion = new DesiredNodes(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        final DesiredNodes desiredNodesWithDifferentHistoryID = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            desiredNodes.version(),
            Collections.emptyList()
        );

        assertThat(desiredNodes.hasSameVersion(desiredNodes), is(equalTo(true)));
        assertThat(desiredNodes.hasSameVersion(desiredNodesWithDifferentVersion), is(equalTo(false)));
        assertThat(desiredNodes.hasSameVersion(desiredNodesWithDifferentHistoryID), is(equalTo(false)));
    }

}
