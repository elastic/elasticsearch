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
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomDefinerView;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomName;
import static org.elasticsearch.cluster.metadata.ViewTestsUtils.randomView;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ViewTests extends AbstractXContentSerializingTestCase<View> {

    private static final TransportVersion DEFINER_RIGHTS = TransportVersion.fromName("esql_view_definer_rights");

    /** A definer-rights view round-trips its rights mode and captured definer identity through both wire and XContent. */
    public void testDefinerViewRoundTrip() throws IOException {
        View original = randomDefinerView(randomName());
        View copy = copyInstance(original);
        assertThat(copy.rightsMode(), equalTo(View.RightsMode.DEFINER));
        assertThat(copy.definer(), equalTo(original.definer()));
        assertThat(copy, equalTo(original));
    }

    /**
     * BWC: serializing a definer-rights view to a node that predates the definer seam (a transport version that does not support
     * {@code esql_view_definer_rights}) must drop the rights axis entirely, so the old node reads it as an invoker view with no
     * definer identity. This is the no-silent-elevation guarantee: an old node never inherits definer fields it cannot police.
     */
    public void testDefinerViewSerializesAsInvokerOnOldNode() throws IOException {
        View definerView = randomDefinerView(randomName());
        TransportVersion old = TransportVersionUtils.randomVersionNotSupporting(DEFINER_RIGHTS);
        View asSeenByOldNode = copyInstance(definerView, old);
        assertThat(asSeenByOldNode.rightsMode(), equalTo(View.RightsMode.INVOKER));
        assertThat(asSeenByOldNode.definer(), nullValue());
        assertThat(asSeenByOldNode.name(), equalTo(definerView.name()));
        assertThat(asSeenByOldNode.query(), equalTo(definerView.query()));
    }

    /** An invoker view round-trips identically across a pre-seam transport version — zero behaviour change for existing views. */
    public void testInvokerViewUnchangedOnOldNode() throws IOException {
        View invokerView = randomView(randomName());
        TransportVersion old = TransportVersionUtils.randomVersionNotSupporting(DEFINER_RIGHTS);
        View asSeenByOldNode = copyInstance(invokerView, old);
        assertThat(asSeenByOldNode.rightsMode(), equalTo(View.RightsMode.INVOKER));
        assertThat(asSeenByOldNode.definer(), nullValue());
        assertThat(asSeenByOldNode, equalTo(invokerView));
    }

    @Override
    protected View doParseInstance(XContentParser parser) throws IOException {
        return View.fromXContent(parser);
    }

    @Override
    protected View createTestInstance() {
        return randomView(randomName());
    }

    @Override
    protected View mutateInstance(View instance) {
        return randomValueOtherThan(instance, () -> randomView(instance.name()));
    }

    @Override
    protected Writeable.Reader<View> instanceReader() {
        return View::new;
    }

    @Override
    protected void assertEqualInstances(View expectedInstance, View newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertEqualViews(expectedInstance, newInstance);
    }

    public static void assertEqualViews(View expectedInstance, View newInstance) {
        assertThat(newInstance.query(), equalTo(expectedInstance.query()));
    }
}
