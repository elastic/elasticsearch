/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomDefinerView;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ViewMetadataTests extends AbstractChunkedSerializingTestCase<ViewMetadata> {

    private static final TransportVersion DEFINER_RIGHTS = TransportVersion.fromName("esql_view_definer_rights");

    /** ViewMetadata carrying a definer view round-trips the rights axis through the wire. */
    public void testDefinerViewMetadataRoundTrip() throws IOException {
        String name = randomName();
        ViewMetadata original = new ViewMetadata(Map.of(name, randomDefinerView(name)));
        ViewMetadata copy = copyInstance(original);
        assertThat(copy, equalTo(original));
        assertThat(copy.getView(name).rightsMode(), equalTo(View.RightsMode.DEFINER));
    }

    /** BWC: a definer view inside ViewMetadata serialized to a pre-seam node is read as invoker, with no definer identity. */
    public void testDefinerViewMetadataDowngradesToInvoker() throws IOException {
        String name = randomName();
        ViewMetadata original = new ViewMetadata(Map.of(name, randomDefinerView(name)));
        TransportVersion old = TransportVersionUtils.randomVersionNotSupporting(DEFINER_RIGHTS);
        ViewMetadata asSeenByOldNode = copyInstance(original, old);
        View view = asSeenByOldNode.getView(name);
        assertThat(view.rightsMode(), equalTo(View.RightsMode.INVOKER));
        assertThat(view.definer(), nullValue());
    }

    @Override
    protected ViewMetadata doParseInstance(XContentParser parser) throws IOException {
        return ViewMetadata.fromXContent(parser);
    }

    @Override
    protected ViewMetadata createTestInstance() {
        return randomViewMetadata();
    }

    @Override
    protected ViewMetadata mutateInstance(ViewMetadata instance) {
        Map<String, View> views = new HashMap<>(instance.views());
        if (views.isEmpty()) {
            String name = randomName();
            return new ViewMetadata(Map.of(name, randomView(name)));
        }
        views.replaceAll((name, view) -> randomValueOtherThan(view, () -> randomView(name)));
        return new ViewMetadata(views);
    }

    private static ViewMetadata randomViewMetadata() {
        int numViews = randomIntBetween(0, 64);
        Map<String, View> views = new HashMap<>(numViews);
        for (int i = 0; i < numViews; i++) {
            final String name = randomName();
            views.put(name, randomView(name));
        }
        return new ViewMetadata(views);
    }

    @Override
    protected Writeable.Reader<ViewMetadata> instanceReader() {
        return ViewMetadata::readFromStream;
    }
}
